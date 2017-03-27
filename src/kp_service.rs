use std::io;

use tokio_service::Service;

use futures::{future, Future, BoxFuture};

use mqtt::packet::VariablePacket;

pub struct Kp;

impl Service for Kp {
    type Request = VariablePacket;
    type Response = VariablePacket;

    type Error = io::Error;

    type Future = BoxFuture<Self::Response, Self::Error>;

    fn call(&self, req: Self::Request) -> Self::Future {
        future::ok(req).boxed()
    }
}
