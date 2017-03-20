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
use std::sync::Arc;

use mqtt::{Encodable, Decodable};
use mqtt::packet::{ConnackPacket, Packet, PingrespPacket, PubackPacket, PublishPacket, PubrecPacket, PubcompPacket, SubackPacket, VariablePacket};
use mqtt::{TopicFilter, QualityOfService};
use mqtt::control::ConnectReturnCode;
use mqtt::packet::suback::SubscribeReturnCode;
use mqtt::packet::publish::QoSWithPacketIdentifier;

mod csv_data_processor;
use csv_data_processor::CsvDataProcessor;

macro_rules! log(
    ($($arg:tt)*) => { {
        let _ = writeln!(&mut ::std::io::stderr(), $($arg)*);
    } }
);

#[derive(Debug)]
enum ZinkError<'a> {
    Mqtt(mqtt::packet::VariablePacketError<'a>),
    NotMqttConnect,
}

impl<'a, P> From<mqtt::packet::PacketError<'a, P>> for ZinkError<'a>
    where P: mqtt::packet::Packet<'a>,
          mqtt::packet::VariablePacketError<'a>: std::convert::From<mqtt::packet::PacketError<'a, P>> {

    fn from(err: mqtt::packet::PacketError<'a, P>) -> ZinkError<'a> {
        ZinkError::Mqtt(mqtt::packet::VariablePacketError::from(err))
    }
}

impl<'a> From<mqtt::packet::VariablePacketError<'a>> for ZinkError<'a> {
    fn from(err: mqtt::packet::VariablePacketError<'a>) -> ZinkError<'a> {
        ZinkError::Mqtt(err)
    }
}

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

    let csv_data_processor = Arc::new(CsvDataProcessor::new(handle, jsonpaths));

    let bind_address = matches.value_of("bind").unwrap();
    let listener = TcpListener::bind(bind_address).unwrap();

    log!("Bound to {}", bind_address);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let csv_data_processor = csv_data_processor.clone();
                thread::spawn(move || {
                    let res = handle_client(stream, &|ref x: &PublishPacket| {
                        csv_data_processor.process_publish(x);
                    });
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

fn handle_client<'a, 'b>(mut stream: TcpStream, process_publish: &'b Fn(&PublishPacket)) -> Result<(), ZinkError<'a>> {
    // This makes .read() call blocking.
    // Otherwise, mqtt decode consumes 100% CPU time.
    let _ = stream.set_read_timeout(None);

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

                process_publish(&x);
            }
            _ => {
            }
        }
    }
}
