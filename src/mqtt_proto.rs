use std::io;

use tokio_proto::pipeline::ServerProto;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::Framed;

use mqtt::packet::VariablePacket;

use mqtt_codec::MqttCodec;

use common::ZinkMessage;

struct MqttTransport<T> {
    upstream: T,
}

impl<T> MqttTransport<T> {
    pub fn new(upstream: T) -> MqttTransport<T> {
        MqttTransport {
            upstream: upstream,
        }
    }
}

impl<T> Stream for MqttTransport<T>
    where T: Stream<Item = VariablePacket, Error = io::Error>,
          T: Sink<Item = VariablePacket, SinkError = io::Error>,
{
    type Item = ZinkMessage;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<String>, io::Error> {
        loop {
            match try_ready!(self.upstream.poll()) {
                Some(VariablePacket::PingreqPacket(_)) => {
                    self.upstream.start_send(PingrespPacket::new())
                }
            }
        }
    }
}

pub struct MqttProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for MqttProto {
    type Request = ZinkMessage;
    type Response = ZinkMessage;

    type Transport = Framed<T, MqttCodec>;

    type BindTransport = Result<Self::Transport, io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(MqttTransport::new(io.framed(MqttCodec)))
    }
}
