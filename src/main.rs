extern crate mqtt;
extern crate serde_json;
extern crate rs_jsonpath;
#[macro_use]
extern crate clap;
extern crate itertools;

use std::net::{TcpListener, TcpStream};
use std::thread;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use itertools::Itertools;

use mqtt::{Encodable, Decodable};
use mqtt::packet::{ConnackPacket, Packet, PingrespPacket, PubackPacket, PubrecPacket, PubcompPacket, SubackPacket, VariablePacket};
use mqtt::{TopicFilter, QualityOfService};
use mqtt::control::ConnectReturnCode;
use mqtt::packet::suback::SubscribeReturnCode;
use mqtt::packet::publish::QoSWithPacketIdentifier;

use serde_json::Value;

use rs_jsonpath::look;

macro_rules! log(
    ($($arg:tt)*) => { {
        let _ = writeln!(&mut ::std::io::stderr(), $($arg)*);
    } }
);

fn main() {
    let matches = clap_app!(zink =>
                            (version: "0.1.0")
                            (@arg file: -f --file +takes_value "File to append result")
                            (@arg JSONPATH: +takes_value "JSON paths to use")
    ).get_matches();

    let jsonpaths: Vec<String> = matches.value_of("JSONPATH").unwrap().split(",").map(String::from).collect();

    let handle: Arc<Mutex<Write + Send>> = if let Some(filepath) = matches.value_of("file") {
        Arc::new(Mutex::new(OpenOptions::new()
                 .append(true)
                 .create(true)
                 .open(filepath)
                 .unwrap())) as Arc<Mutex<Write + Send>>
    } else {
        Arc::new(Mutex::new(io::stdout())) as Arc<Mutex<Write + Send>>
    };

    let listener = TcpListener::bind("0.0.0.0:1883").unwrap();

    log!("Listening on port 1883");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let paths = jsonpaths.clone();
                let handle = handle.clone();
                thread::spawn(move || {
                    handle_client(stream, &paths, handle);
                });
            }
            Err(e) => {
                log!("{}", e);
            }
        }
    }
}

fn sub_to_ack(&(_, qos): &(TopicFilter, QualityOfService)) -> SubscribeReturnCode {
    match qos {
        QualityOfService::Level0 => SubscribeReturnCode::MaximumQoSLevel0,
        QualityOfService::Level1 => SubscribeReturnCode::MaximumQoSLevel1,
        QualityOfService::Level2 => SubscribeReturnCode::MaximumQoSLevel1,
    }
}

fn process_entries(entries: &Vec<Value>, jsonpaths: &Vec<String>, handle: Arc<Mutex<Write>>) {
    for entry in entries.into_iter() {
        let mut csv = jsonpaths.iter()
            .map(|path| {
                let res = look(&entry, &entry, path.clone()).unwrap_or("".to_string());
                if res != "[]" { res } else { "".to_string() }
            })
            .join(",");
        csv.push('\n');

        let mut handle = handle.lock().unwrap();
        let _ = handle.write_all(csv.as_bytes());
    }
}

fn handle_client(mut stream: TcpStream, jsonpaths: &Vec<String>, handle: Arc<Mutex<Write>>) {
    // This makes .read() call blocking.
    // Otherwise, mqtt decode consumes 100% CPU time.
    let _ = stream.set_read_timeout(None);

    log!("{:?}", stream);
    if let Ok(VariablePacket::ConnectPacket(x)) = VariablePacket::decode(&mut stream) {
        log!("{:?}", x);
        ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted).encode(&mut stream);
    } else {
        return;
    }

    loop {
        let parse_result = VariablePacket::decode(&mut stream);
        if let Ok(packet) = parse_result {
            log!("{:?}", packet);
            match packet {
                VariablePacket::SubscribePacket(x) => {
                    SubackPacket::new(
                        x.packet_identifier(),
                        x.payload().subscribes().into_iter().map(sub_to_ack).collect()
                    ).encode(&mut stream);
                }
                VariablePacket::PingreqPacket(_) => {
                    PingrespPacket::new()
                        .encode(&mut stream);
                }
                VariablePacket::PubrelPacket(x) => {
                    PubcompPacket::new(x.packet_identifier())
                        .encode(&mut stream);
                }
                VariablePacket::PublishPacket(x) => {
                    match x.qos() {
                        QoSWithPacketIdentifier::Level0 => {
                            // No additionl handling required
                        }
                        QoSWithPacketIdentifier::Level1(pkid) => {
                            PubackPacket::new(pkid)
                                .encode(&mut stream);
                        }
                        QoSWithPacketIdentifier::Level2(pkid) => {
                            PubrecPacket::new(pkid)
                                .encode(&mut stream);
                        }
                    }

                    if let Ok(payload) = std::str::from_utf8(x.payload()) {
                        log!("{}: {}", x.topic_name(), payload);
                    } else {
                        log!("{}: {:?}", x.topic_name(), x.payload());
                    }

                    match serde_json::from_slice(x.payload()) {
                        Ok(Value::Object(obj)) => {
                            if let Some(&Value::Array(ref entries)) = obj.get("entries") {
                                process_entries(entries, jsonpaths, handle.clone());
                            } else {
                                // TODO: error handling
                            }
                        }
                        Ok(Value::Array(entries)) => {
                            process_entries(&entries, jsonpaths, handle.clone());
                        }
                        _ => {
                            log!("Parse error");
                        }
                    }
                }
                _ => {
                }
            }
        }
    }
}
