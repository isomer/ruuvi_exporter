use btleplug::api::{Central, CentralEvent, Manager as _, ScanFilter};
use btleplug::platform::Manager;
use futures::stream::StreamExt;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use lazy_static::lazy_static;
use prometheus::{register_gauge_vec, Encoder as _, TextEncoder};
use std::convert::Infallible;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

lazy_static! {
    static ref TEMP_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("temperature_degC", "Temperature in degrees C", &["mac"]).unwrap();
    static ref HUMIDITY_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("humidity_perc", "Humidity in %", &["mac"]).unwrap();
    static ref PRESSURE_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("pressure_pa", "Pressure in Pa", &["mac"]).unwrap();
    static ref ACCEL_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("accel_mG", "Acceleration in mG", &["mac", "axis"]).unwrap();
    static ref BATTERY_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("battery_V", "Battery Voltage in Volts", &["mac"]).unwrap();
    static ref TX_POWER_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("tx_power_dBm", "TX Power in dBm", &["mac"]).unwrap();
    static ref MOVEMENTS_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("movements_count", "Movement Count", &["mac"]).unwrap();
    static ref SEQUENCE_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("sequence_count", "Sequence", &["mac"]).unwrap();
    static ref LAST_SEEN_GAUGE: prometheus::GaugeVec =
        register_gauge_vec!("last_seen_ts", "Timestamp (Secs past Epoch)", &["mac"]).unwrap();
}

#[derive(Clone, Copy)]
struct Ruuvi {
    version: u8,
    /// Temperature in 0.005 degrees
    temp: i16,
    /// Humidity (16bit unsigned) in 0.0025% (0-163.83% range, though realistically 0-100%)
    humidity: u16,
    /// Pressure (16bit unsigned) in 1 Pa units, with offset of -50 000 Pa
    pressure: u16,
    /// Acceleration-X
    accel_x: i16,
    /// Acceleration-Y
    accel_y: i16,
    /// Acceleration-Z
    accel_z: i16,
    /// battery voltage above 1.6V, in millivolts (1.6V to 3.646V range)
    battery: u16,
    /// TX power above -40dBm, in 2dBm steps. (-40dBm to +20dBm range)
    tx_power: u8,
    /// Movement counter, incremented by motion detection interrupts from accelerometer
    movements: u8,
    /// Measurement sequence number, each time a measurement is taken, this is incremented by one,
    /// used for measurement de-duplication. Depending on the transmit interval, multiple packets
    /// with the same measurements can be sent, and there may be measurements that never were sent.
    seq: u16,
    /// 48bit MAC address.
    mac: [u8; 6],
}

impl Ruuvi {
    pub fn get_version(&self) -> u8 {
        self.version
    }
    /// Temperature (°C)
    pub fn get_temp(&self) -> Option<f64> {
        if self.temp == i16::MIN {
            None
        } else {
            Some(self.temp as f64 * 0.005)
        }
    }
    /// Humidity (%)
    pub fn get_humidity(&self) -> Option<f64> {
        if self.humidity == 65535 {
            None
        } else {
            Some(self.humidity as f64 * 0.0025)
        }
    }
    /// Atmospheric Pressure (Pa)
    pub fn get_pressure(&self) -> Option<i32> {
        if self.pressure == 65535 {
            None
        } else {
            Some(self.pressure as i32 - 50000)
        }
    }
    fn preprocess_accel(accel: i16) -> Option<i16> {
        if accel == i16::from_be_bytes([0x80, 0x00]) {
            None
        } else {
            Some(accel)
        }
    }
    /// Acceleration (mG)
    pub fn get_acceleration(&self) -> Option<(i16, i16, i16)> {
        match (
            Self::preprocess_accel(self.accel_x),
            Self::preprocess_accel(self.accel_y),
            Self::preprocess_accel(self.accel_z),
        ) {
            (Some(x), Some(y), Some(z)) => Some((x, y, z)),
            _ => None,
        }
    }
    /// Battery voltage (V)
    pub fn get_voltage(&self) -> Option<f64> {
        if self.battery == 2047 {
            None
        } else {
            Some((1600.0 + self.battery as f64) / 1000.0)
        }
    }
    /// TX Power (dBm)
    pub fn get_tx_power(&self) -> Option<i8> {
        if self.tx_power == 31 {
            None
        } else {
            Some(-40 + (self.tx_power as i8) * 2)
        }
    }
    pub fn get_movements(&self) -> u8 {
        self.movements
    }
    /// Sequence Number
    pub fn get_sequence(&self) -> Option<u16> {
        if self.seq == 65535 {
            None
        } else {
            Some(self.seq)
        }
    }
}

impl std::fmt::Debug for Ruuvi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Debug")
            .field("version", &self.get_version())
            .field("temperature", &self.get_temp())
            .field("humidity", &self.get_humidity())
            .field("pressure", &self.get_pressure())
            .field("accelation", &self.get_acceleration())
            .field("voltage", &self.get_voltage())
            .field("tx_power", &self.get_tx_power())
            .field("sequence", &self.get_sequence())
            .field("movements", &self.movements)
            .field("mac", &self.mac)
            .finish()
    }
}

trait ReadData {
    fn read_const_bytes<const N: usize>(&mut self) -> std::io::Result<[u8; N]>;
    fn read_u8(&mut self) -> std::io::Result<u8>;
    fn read_u16be(&mut self) -> std::io::Result<u16>;
    fn read_i16be(&mut self) -> std::io::Result<i16>;
}

impl<T: std::io::Read> ReadData for T {
    fn read_const_bytes<const N: usize>(&mut self) -> std::io::Result<[u8; N]> {
        let mut buf = [0_u8; N];
        self.read_exact(&mut buf)?;
        Ok(buf)
    }
    fn read_u8(&mut self) -> std::io::Result<u8> {
        Ok(self.read_const_bytes::<1>()?[0])
    }
    fn read_u16be(&mut self) -> std::io::Result<u16> {
        Ok(u16::from_be_bytes(self.read_const_bytes()?))
    }
    fn read_i16be(&mut self) -> std::io::Result<i16> {
        Ok(i16::from_be_bytes(self.read_const_bytes()?))
    }
}

impl TryFrom<&[u8]> for Ruuvi {
    type Error = std::io::Error;
    fn try_from(v: &[u8]) -> Result<Self, Self::Error> {
        let mut cur = std::io::Cursor::new(v);
        let version = cur.read_u8()?;
        let temp = cur.read_i16be()?;
        let humidity = cur.read_u16be()?;
        let pressure = cur.read_u16be()?;
        let accel_x = cur.read_i16be()?;
        let accel_y = cur.read_i16be()?;
        let accel_z = cur.read_i16be()?;
        let power = cur.read_u16be()?;
        let battery = power >> (16 - 11);
        let tx_power = (power & (0b1_1111)) as u8;
        let movements = cur.read_u8()?;
        let seq = cur.read_u16be()?;
        let mac = cur.read_const_bytes()?;
        Ok(Ruuvi {
            version,
            temp,
            humidity,
            pressure,
            accel_x,
            accel_y,
            accel_z,
            battery,
            tx_power,
            movements,
            seq,
            mac,
        })
    }
}

fn format_mac(mac: &[u8; 6]) -> String {
    mac.map(|x| format!("{x:<02x}")).join(":")
}

async fn gather_data() -> Result<(), btleplug::Error> {
    let mut ruuvi_data: std::collections::HashMap<[u8; 6], (std::time::SystemTime, Ruuvi)> =
        Default::default();
    let manager = Manager::new().await?;

    // get bluetooth adapter
    let adapters = manager.adapters().await?;

    let adapter = adapters.into_iter().nth(0).unwrap();

    tracing::info!("Listening on {:?}", adapter);

    let mut events = adapter.events().await?;

    // start scanning for devices
    adapter.start_scan(ScanFilter::default()).await?;

    while let Some(event) = events.next().await {
        match event {
            CentralEvent::DeviceUpdated(_) => (),
            CentralEvent::ManufacturerDataAdvertisement {
                manufacturer_data, ..
            } if manufacturer_data.contains_key(&0x0499) => {
                if let Some(ruuvi) = manufacturer_data
                    .get(&0x0499)
                    .and_then(|x| Ruuvi::try_from(x.as_slice()).ok())
                {
                    tracing::trace!("Parsed Ruuvi Data = {:?}", ruuvi);
                    let (last_seen, data) = ruuvi_data
                        .entry(ruuvi.mac)
                        .or_insert_with(|| (std::time::SystemTime::now(), ruuvi.clone()));
                    if data.seq != ruuvi.seq {
                        *last_seen = std::time::SystemTime::now();
                        *data = ruuvi;
                        let mac = format_mac(&data.mac);
                        ruuvi
                            .get_temp()
                            .map(|t| TEMP_GAUGE.with_label_values(&[&mac]).set(t));
                        ruuvi
                            .get_humidity()
                            .map(|h| HUMIDITY_GAUGE.with_label_values(&[&mac]).set(h));
                        ruuvi
                            .get_pressure()
                            .map(|p| PRESSURE_GAUGE.with_label_values(&[&mac]).set(p as f64));
                        ruuvi
                            .get_voltage()
                            .map(|b| BATTERY_GAUGE.with_label_values(&[&mac]).set(b as f64));
                        ruuvi
                            .get_tx_power()
                            .map(|p| TX_POWER_GAUGE.with_label_values(&[&mac]).set(p as f64));
                        ruuvi
                            .get_sequence()
                            .map(|s| SEQUENCE_GAUGE.with_label_values(&[&mac]).set(s as f64));
                        MOVEMENTS_GAUGE
                            .with_label_values(&[&mac])
                            .set(ruuvi.get_movements() as f64);
                        ruuvi.get_acceleration().map(|(x, y, z)| {
                            ACCEL_GAUGE.with_label_values(&[&mac, "x"]).set(x as f64);
                            ACCEL_GAUGE.with_label_values(&[&mac, "y"]).set(y as f64);
                            ACCEL_GAUGE.with_label_values(&[&mac, "z"]).set(z as f64);
                        });
                        LAST_SEEN_GAUGE.with_label_values(&[&mac]).set(
                            last_seen
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64(),
                        );
                    }
                } else {
                    tracing::debug!("Raw Manufacturer Data: {:?}", manufacturer_data);
                }
            }
            ev => tracing::debug!("Event = {:?}", ev),
        }
    }

    adapter.stop_scan().await?;
    // start scanning for devices
    Ok(())
}

async fn gather_data_wrapper() {
    gather_data().await.unwrap()
}

async fn export_metrics(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();

    // Gather the metrics.
    let metric_families = prometheus::gather();
    // Encode them to send.
    encoder.encode(&metric_families, &mut buffer).unwrap();

    let output = String::from_utf8(buffer).unwrap();
    Ok(Response::new(Body::from(output)))
}

async fn export_data() {
    // Construct our SocketAddr to listen on...
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 3001));

    // And a MakeService to handle each connection...
    let make_service =
        make_service_fn(|_conn| async { Ok::<_, Infallible>(service_fn(export_metrics)) });

    // Then bind and serve...
    let server = Server::bind(&addr).serve(make_service);

    tracing::info!("Listening on {}", addr);

    // And run forever...
    if let Err(e) = server.await {
        tracing::error!("server error: {}", e);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let export = tokio::spawn(export_data());
    let gather = tokio::spawn(gather_data_wrapper());
    export.await?;
    gather.await?;
    Ok(())
}
