use std::io::Write;

use serde_json::{self, Value};

use rs_jsonpath::look;

use hyper::client::Client;

use application::Extension;

macro_rules! log(
    ($($arg:tt)*) => { {
        let _ = writeln!(&mut ::std::io::stderr(), $($arg)*);
    } }
);

pub struct InfluxDbDataProcessor {
    client: Client,
}

impl InfluxDbDataProcessor {
    pub fn new() -> InfluxDbDataProcessor {
        InfluxDbDataProcessor {
            client: Client::new(),
        }
    }

    fn process_entries(&self, entries: &Vec<Value>) {
        for entry in entries.into_iter() {
            let timestamp = look(&entry, &entry, "timestamp".to_string());
            let tag_id = look(&entry, &entry, "tagId".to_string());
            let battery_level = look(&entry, &entry, "batteryLevel".to_string());
            let temperature = look(&entry, &entry, "temperature".to_string());

            match (timestamp, tag_id, battery_level, temperature) {
                (Ok(timestamp), Ok(tag_id), Ok(battery_level), Ok(temperature)) => {
                    let body = format!("sensor,tag={} temperature={},batteryLevel={} {}",
                            tag_id, temperature, battery_level, timestamp);
                    log!("{}", body);

                    let res = self.client
                        .post("http://127.0.0.1:8086/write?db=zink_sensor&precision=ms")
                        .body(&body)
                        .send();
                    log!("{:?}", res);
                },
                _ => {
                },
            }

        }
    }
}

impl Extension for InfluxDbDataProcessor {
    fn handle_request(&self, _path: &[&str], payload: &[u8]) {
        match serde_json::from_slice(payload) {
            Ok(Value::Object(obj)) => {
                if let Some(&Value::Array(ref entries)) = obj.get("entries") {
                    self.process_entries(entries);
                } else {
                    // TODO: error handling
                }
            }
            Ok(Value::Array(entries)) => {
                self.process_entries(&entries);
            }
            _ => {
                log!("Parse error");
            }
        }
    }
}

unsafe impl Sync for InfluxDbDataProcessor {
}
