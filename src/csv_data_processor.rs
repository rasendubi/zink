use std::io::Write;
use std::sync::Mutex;

use mqtt::packet::{Packet, PublishPacket};

use serde_json::{self, Value};

use rs_jsonpath::look;

use itertools::Itertools;

macro_rules! log(
    ($($arg:tt)*) => { {
        let _ = writeln!(&mut ::std::io::stderr(), $($arg)*);
    } }
);

pub struct CsvDataProcessor<W> {
    handle: Mutex<W>,
    patterns: Vec<String>,
}

impl<W> CsvDataProcessor<W> where W: Write {
    pub fn new(handle: W, patterns: Vec<String>) -> CsvDataProcessor<W> {
        CsvDataProcessor {
            handle: Mutex::new(handle),
            patterns: patterns,
        }
    }

    pub fn process_publish(&self, publish: &PublishPacket) {
        match serde_json::from_slice(publish.payload()) {
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

    fn process_entries(&self, entries: &Vec<Value>) {
        for entry in entries.into_iter() {
            let mut csv = self.patterns.iter()
                .map(|path| {
                    let res = look(&entry, &entry, path.clone()).unwrap_or("".to_string());
                    if res != "[]" { res } else { "".to_string() }
                })
                .join(",");

            log!("Writing '{}'", csv);

            csv.push('\n');

            let mut handle = self.handle.lock().unwrap();
            let _ = handle.write_all(csv.as_bytes());
        }
    }
}

unsafe impl<W> Sync for CsvDataProcessor<W> {
}
