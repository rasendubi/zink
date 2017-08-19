extern crate mqtt;
extern crate serde_json;
extern crate rs_jsonpath;
#[macro_use]
extern crate clap;
extern crate itertools;
extern crate hyper;

use std::net::{TcpListener, TcpStream};
use std::thread;
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::sync::Arc;

use mqtt::{Encodable, Decodable};
use mqtt::packet::{ConnackPacket, Packet, PingrespPacket, PubackPacket, PubrecPacket, PubcompPacket, SubackPacket, VariablePacket};
use mqtt::{TopicFilter, QualityOfService};
use mqtt::control::ConnectReturnCode;
use mqtt::packet::suback::SubscribeReturnCode;
use mqtt::packet::publish::QoSWithPacketIdentifier;

#[macro_use]
mod common;
mod application;
mod csv_data_processor;
mod influxdb_data_processor;

use common::ZinkError;
use application::Application;
use csv_data_processor::CsvDataProcessor;
use influxdb_data_processor::InfluxDbDataProcessor;

fn main() {
    let matches = clap_app!(
        zink =>
            (version: "0.1.0")
            (@arg file: -f --file +takes_value "File to append result to. If not specified, send results to stdout")
            (@arg bind: -b --bind +takes_value default_value("0.0.0.0:1883") "Bind address and port")
            (@arg JSONPATH: +required "JSON paths to use")
    ).get_matches();

    let jsonpaths: Vec<String> = matches.value_of("JSONPATH").unwrap().split(",").map(String::from).collect();

    let handle: Box<Write + Send> = if let Some(filepath) = matches.value_of("file") {
        Box::new(OpenOptions::new()
                 .append(true)
                 .create(true)
                 .open(filepath)
                 .unwrap()) as Box<Write + Send>
    } else {
        Box::new(io::stdout()) as Box<Write + Send>
    };

    let mut application = Application::new();
    application.register_extension("dcx-instance-1", Box::new(CsvDataProcessor::new(handle, jsonpaths)));
    application.register_extension("dcx-instance-1", Box::new(InfluxDbDataProcessor::new()));
    application.register_extension("dcx_instance_1", Box::new(InfluxDbDataProcessor::new()));
    let application = Arc::new(application);

    let bind_address = matches.value_of("bind").unwrap();
    let listener = TcpListener::bind(bind_address).unwrap();

    log!("Bound to {}", bind_address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let application = application.clone();
                thread::spawn(move || {
                    let res = handle_client(stream, &application);
                    log!("handle_client exited with {:?}", res);
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

fn handle_client<'a, 'b>(mut stream: TcpStream, app: &Application) -> Result<(), ZinkError<'a>> {
    log!("{:?}", stream);
    if let Ok(VariablePacket::ConnectPacket(x)) = VariablePacket::decode(&mut stream) {
        log!("{:?}", x);
        try!(ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted).encode(&mut stream));
    } else {
        return Err(ZinkError::NotMqttConnect);
    }

    loop {
        let packet = try!(VariablePacket::decode(&mut stream));
        log!("{:?}", packet);
        match packet {
            VariablePacket::DisconnectPacket(_) => {
                return Ok(());
            }
            VariablePacket::SubscribePacket(x) => {
                try!(SubackPacket::new(
                    x.packet_identifier(),
                    x.payload().subscribes().into_iter().map(sub_to_ack).collect()
                ).encode(&mut stream));
            }
            VariablePacket::PingreqPacket(_) => {
                try!(PingrespPacket::new()
                     .encode(&mut stream));
            }
            VariablePacket::PubrelPacket(x) => {
                try!(PubcompPacket::new(x.packet_identifier())
                     .encode(&mut stream));
            }
            VariablePacket::PublishPacket(x) => {
                match x.qos() {
                    QoSWithPacketIdentifier::Level0 => {
                        // No additional handling is required
                    }
                    QoSWithPacketIdentifier::Level1(pkid) => {
                        try!(PubackPacket::new(pkid)
                             .encode(&mut stream));
                    }
                    QoSWithPacketIdentifier::Level2(pkid) => {
                        try!(PubrecPacket::new(pkid)
                             .encode(&mut stream));
                    }
                }

                if let Ok(payload) = std::str::from_utf8(x.payload()) {
                    log!("{}: {}", x.topic_name(), payload);
                } else {
                    log!("{}: {:?}", x.topic_name(), x.payload());
                }

                let topic_name = x.topic_name().split('/').collect::<Vec<_>>();
                if topic_name.len() >= 2 {
                    // ("kaa/demo1", "dcx-instance-1/endpoint/json")
                    let (_, path) = topic_name.split_at(2);

                    app.handle_request(
                        path,
                        x.payload(),
                    );
                }
            }
            _ => {
            }
        }
    }
}
