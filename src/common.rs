use mqtt;

macro_rules! log(
    ($($arg:tt)*) => { {
        let _ = writeln!(&mut ::std::io::stderr(), $($arg)*);
    } }
);

#[derive(Debug)]
pub enum ZinkError<'a> {
    Mqtt(mqtt::packet::VariablePacketError<'a>),
    NotMqttConnect,
}

impl<'a, P> From<mqtt::packet::PacketError<'a, P>> for ZinkError<'a>
    where P: mqtt::packet::Packet<'a>,
          mqtt::packet::VariablePacketError<'a>: ::std::convert::From<mqtt::packet::PacketError<'a, P>> {

    fn from(err: mqtt::packet::PacketError<'a, P>) -> ZinkError<'a> {
        ZinkError::Mqtt(mqtt::packet::VariablePacketError::from(err))
    }
}

impl<'a> From<mqtt::packet::VariablePacketError<'a>> for ZinkError<'a> {
    fn from(err: mqtt::packet::VariablePacketError<'a>) -> ZinkError<'a> {
        ZinkError::Mqtt(err)
    }
}

pub struct ZinkMessage {
    pub path: String,
    pub payload: Vec<u8>,
}
