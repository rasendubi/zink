use std::io::{self, Cursor};
use bytes::{BytesMut, BufMut};
use tokio_io::codec::{Encoder, Decoder};

use mqtt::packet::{VariablePacket, VariablePacketError};
use mqtt::{Decodable, Encodable};

pub struct MqttCodec;

impl Decoder for MqttCodec {
    type Item = VariablePacket;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<VariablePacket>> {
        let mut cursor = Cursor::new(&buf[..]);
        match VariablePacket::decode(&mut cursor) {
            Ok(result) => {
                buf.split_at(cursor.position() as usize);
                Ok(Some(result))
            }
            Err(VariablePacketError::IoError(_)) => {
                Ok(None)
            }
            Err(err) => {
                Err(io::Error::new(io::ErrorKind::Other,
                                   err))
            }
        }
    }
}

impl Encoder for MqttCodec {
    type Item = VariablePacket;
    type Error = io::Error;

    fn encode(&mut self, msg: VariablePacket, buf: &mut BytesMut) -> io::Result<()> {
        match msg.encode(&mut buf.writer()) {
            Ok(_) => Ok(()),
            Err(err) => Err(io::Error::new(io::ErrorKind::Other, err))
        }
    }
}
