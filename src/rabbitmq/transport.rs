//! Own a shutdown handle independently of lapin's handshake/channel handles.
//! Dropping a handshake future alone does not terminate lapin's IO loop.
use async_compat::Compat;
use lapin::{
    AsyncTcpStream, Connection, ConnectionProperties,
    tcp::TLSConfig,
    uri::{AMQPScheme, AMQPUri},
};
use std::{
    io,
    net::{Shutdown, TcpStream},
    sync::{Arc, Mutex},
};

use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct State {
    stopped: bool,
    socket: Option<TcpStream>,
}
#[derive(Default)]
pub(super) struct Transport {
    state: Mutex<State>,
    cancelled: CancellationToken,
}
impl Transport {
    fn register(&self, socket: TcpStream) -> io::Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.stopped {
            let _ = socket.shutdown(Shutdown::Both);
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "transport stopped",
            ));
        }
        state.socket = Some(socket);
        Ok(())
    }
    pub fn stop(&self) {
        self.cancelled.cancel();
        let mut state = self.state.lock().unwrap();
        state.stopped = true;
        if let Some(socket) = state.socket.take() {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }
    pub async fn connect(self: Arc<Self>, uri: AMQPUri) -> lapin::Result<Connection> {
        let owner = self.clone();
        let result = Connection::connector(
            uri,
            lapin::runtime::default_runtime()?,
            async move |uri: AMQPUri, _runtime| {
                tokio::select! {
                    biased;
                    _ = owner.cancelled.cancelled() => Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted, "transport stopped",
                    ).into()),
                    result = async {
                let socket = tokio::net::TcpStream::connect((
                    uri.authority.host.as_str(),
                    uri.authority.port,
                ))
                .await?;
                let socket = socket.into_std()?;
                owner.register(socket.try_clone()?)?;
                let stream =
                    AsyncTcpStream::Plain(Compat::new(tokio::net::TcpStream::from_std(socket)?));
                match uri.scheme {
                    AMQPScheme::AMQP => Ok(stream),
                    AMQPScheme::AMQPS => Ok(stream
                        .into_tls(&uri.authority.host, TLSConfig::default())
                        .await?),
                }
                    } => result,
                }
            },
            ConnectionProperties::default().with_connection_name("channels-manager-rust".into()),
        )
        .await;
        if result.is_err() {
            self.stop();
        }
        result
    }
}
impl Drop for Transport {
    fn drop(&mut self) {
        self.stop();
    }
}
