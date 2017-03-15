extern crate mqtt;

use std::net::{TcpListener, TcpStream};
use std::thread;

use mqtt::{Encodable, Decodable};
use mqtt::packet::{ConnackPacket, Packet, PingrespPacket, SubackPacket, VariablePacket};
use mqtt::{TopicFilter, QualityOfService};
use mqtt::control::{ConnectReturnCode};
use mqtt::packet::suback::{SubscribeReturnCode};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:1883").unwrap();

    println!("Listening on port 1883");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("{}", e);
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

fn handle_client(mut stream: TcpStream) {
    // This makes .read() call blocking.
    // Otherwise, mqtt decode consumes 100% CPU time.
    stream.set_read_timeout(None);

    println!("{:?}", stream);
    let auto_decode = VariablePacket::decode(&mut stream);
    println!("{:?}", auto_decode);
    ConnackPacket::new(false, ConnectReturnCode::ConnectionAccepted).encode(&mut stream);

    loop {
        let parse_result = VariablePacket::decode(&mut stream);
        if let Ok(packet) = parse_result {
            match packet {
                VariablePacket::SubscribePacket(x) => {
                    println!("{:?}", x);
                    SubackPacket::new(
                        x.packet_identifier(),
                        x.payload().subscribes().into_iter().map(sub_to_ack).collect()
                    ).encode(&mut stream);
                }
                VariablePacket::PingreqPacket(x) => {
                    println!("{:?}", x);
                    PingrespPacket::new()
                        .encode(&mut stream);
                }
                VariablePacket::PublishPacket(x) => {
                    println!("{:?}", x);
                    println!("Got publish");
                    println!("Topic name: {}", x.topic_name());
                    println!("Payload: {}", std::str::from_utf8(x.payload()).unwrap_or("<DECODING ERROR>"));
                }
                x => {
                    println!("{:?}", x);
                }
            }
        }
    }
}
