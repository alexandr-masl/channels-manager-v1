use super::{Publisher, RabbitConsumer, RabbitError, Session, transport::Transport};
use crate::{config::RabbitMqConfig, contracts::rabbitmq::*};
use futures_util::StreamExt;
use lapin::{
    Channel, ChannelStatus, Connection, Event,
    options::*,
    types::{AMQPValue, FieldTable},
    uri::AMQPUri,
};
use serde_json::Value;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::Semaphore, task::JoinHandle, time::timeout};

pub fn queue_arguments(value: &Value) -> Result<FieldTable, RabbitError> {
    let object = value.as_object().ok_or(RabbitError::Topology)?;
    let mut table = FieldTable::default();
    for (key, value) in object {
        let value = match value {
            Value::String(v) => AMQPValue::LongString(v.as_str().into()),
            Value::Number(v) => AMQPValue::LongInt(
                v.as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .ok_or(RabbitError::Topology)?,
            ),
            _ => return Err(RabbitError::Topology),
        };
        table.insert(key.as_str().into(), value);
    }
    Ok(table)
}
pub struct RabbitMq {
    config: RabbitMqConfig,
    timeout: Duration,
    connection: Option<Connection>,
    transport: Arc<Transport>,
    connecting: Option<JoinHandle<Result<Connection, RabbitError>>>,
    declarations: Option<Channel>,
    publisher: Option<Publisher>,
    consumer_channel: Option<Channel>,
    consumer_tag: Option<String>,
    pub(crate) session: Arc<Session>,
    channels: Arc<Mutex<Vec<ChannelStatus>>>,
    monitor: Option<JoinHandle<()>>,
}
impl RabbitMq {
    pub fn new(config: RabbitMqConfig, timeout: Duration) -> Self {
        Self {
            config,
            timeout,
            connection: None,
            transport: Arc::new(Transport::default()),
            connecting: None,
            declarations: None,
            publisher: None,
            consumer_channel: None,
            consumer_tag: None,
            session: Session::new(),
            channels: Arc::new(Mutex::new(Vec::new())),
            monitor: None,
        }
    }
    pub fn heartbeat_seconds(&self) -> u16 {
        self.connection
            .as_ref()
            .map_or(0, |c| c.configuration().heartbeat())
    }
    pub fn publisher(&self) -> Result<Publisher, RabbitError> {
        self.session.check()?;
        self.publisher
            .as_ref()
            .filter(|p| p.channel.status().connected() && p.channel.status().confirm())
            .cloned()
            .ok_or(RabbitError::Unavailable)
    }
    pub async fn connect_and_declare(&mut self) -> Result<(), RabbitError> {
        if self.session.closed.is_cancelled() {
            *self = Self::new(self.config.clone(), self.timeout);
        }
        self.session.check()?;
        if self.connection.is_none() {
            if self.connecting.is_none() {
                let mut uri: AMQPUri = self
                    .config
                    .uri
                    .expose()
                    .parse()
                    .map_err(|_| RabbitError::InvalidConfiguration)?;
                uri.query.heartbeat = Some(self.config.heartbeat_seconds.get());
                uri.query.connection_timeout = Some(self.timeout.as_millis() as u64);
                let limit = self.timeout;
                self.transport = Arc::new(Transport::default());
                let transport = self.transport.clone();
                self.connecting = Some(tokio::spawn(async move {
                    let result = timeout(limit, transport.clone().connect(uri)).await;
                    match result {
                        Ok(result) => result.map_err(classify),
                        Err(_) => {
                            transport.stop();
                            Err(RabbitError::Timeout)
                        }
                    }
                }));
            }
            let result = self.connecting.as_mut().unwrap().await;
            self.connecting = None;
            self.connection = Some(result.map_err(|_| RabbitError::Unavailable)??);
            self.start_monitor();
        }
        if self.declarations.is_none() {
            let channel = bounded(
                self.timeout,
                self.connection.as_ref().unwrap().create_channel(),
            )
            .await?;
            self.channels.lock().unwrap().push(channel.status().clone());
            self.declarations = Some(channel);
        }
        for contract in queue_contracts(&self.config) {
            let result = bounded(
                self.timeout,
                self.declarations.as_ref().unwrap().queue_declare(
                    contract.name.into(),
                    QueueDeclareOptions {
                        durable: contract.durable,
                        exclusive: contract.exclusive,
                        auto_delete: contract.auto_delete,
                        ..Default::default()
                    },
                    queue_arguments(&contract.arguments)?,
                ),
            )
            .await;
            if let Err(error) = result {
                self.session.fail(error);
                return Err(error);
            }
        }
        self.session.check()
    }
    pub async fn initialize_publisher(&mut self) -> Result<(), RabbitError> {
        self.session.check()?;
        if self.publisher.is_none() {
            let connection = self.connection.as_ref().ok_or(RabbitError::Unavailable)?;
            let channel = bounded(self.timeout, connection.create_channel()).await?;
            self.channels.lock().unwrap().push(channel.status().clone());
            self.publisher = Some(Publisher {
                channel,
                session: self.session.clone(),
                permits: Arc::new(Semaphore::new(self.config.prefetch.get() as usize)),
                capacity: self.config.prefetch.get() as u32,
                timeout: self.config.publish_timeout,
                trade_queue: self.config.output_queue.clone(),
                retry_queue: format!("{}{RETRY_SUFFIX}", self.config.input_queue),
            });
        }
        let result = bounded(
            self.timeout,
            self.publisher
                .as_ref()
                .unwrap()
                .channel
                .confirm_select(ConfirmSelectOptions::default()),
        )
        .await;
        if let Err(error) = result {
            self.session.fail(error);
            return Err(error);
        }
        self.session.check()
    }
    pub async fn start_consumer(&mut self) -> Result<RabbitConsumer, RabbitError> {
        self.session.check()?;
        if self.consumer_tag.is_some() || self.session.intake.is_cancelled() {
            return Err(RabbitError::Unavailable);
        }
        self.publisher()?;
        if self.consumer_channel.is_none() {
            let channel = bounded(
                self.timeout,
                self.connection
                    .as_ref()
                    .ok_or(RabbitError::Unavailable)?
                    .create_channel(),
            )
            .await?;
            self.channels.lock().unwrap().push(channel.status().clone());
            self.consumer_channel = Some(channel);
        }
        let channel = self.consumer_channel.as_ref().unwrap();
        bounded(
            self.timeout,
            channel.basic_qos(
                self.config.prefetch.get(),
                BasicQosOptions { global: false },
            ),
        )
        .await?;
        let tag = format!("channels-manager-{}", uuid::Uuid::new_v4());
        self.consumer_tag = Some(tag.clone());
        let consumer = bounded(
            self.timeout,
            channel.basic_consume(
                self.config.input_queue.into(),
                tag.into(),
                BasicConsumeOptions {
                    no_ack: false,
                    ..Default::default()
                },
                FieldTable::default(),
            ),
        )
        .await?;
        self.session.check()?;
        println!("Listening for messages on {}.", self.config.input_queue);
        Ok(RabbitConsumer {
            consumer,
            session: self.session.clone(),
            permits: Arc::new(Semaphore::new(self.config.prefetch.get() as usize)),
            timeout: self.timeout,
        })
    }
    fn start_monitor(&mut self) {
        let connection = self.connection.as_ref().unwrap();
        let mut events = Box::pin(connection.events_listener());
        let status = connection.status().clone();
        let channels = self.channels.clone();
        let session = self.session.clone();
        self.monitor = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! { biased;
                    _=session.closed.cancelled()=>break,
                    event=events.next()=>match event {
                        Some(Event::Error(_)|Event::ConnectionBlocked(_)|Event::SendFlow(false))|None=>{session.fail(RabbitError::Unavailable);break;},
                        _=>{},
                    },
                    _=interval.tick()=> {
                        if !status.connected() || channels.lock().unwrap().iter().any(|c|!c.connected()) {
                            session.fail(RabbitError::Unavailable);break;
                        }
                    }
                }
            }
        }));
    }
    pub fn quiesce(&self) {
        self.session.intake.cancel();
    }
    pub async fn wait_for_failure(&self) -> RabbitError {
        self.session.wait_for_failure().await
    }
    pub async fn stop_consumers(&mut self) -> Result<(), RabbitError> {
        self.quiesce();
        if let (Some(channel), Some(tag)) = (&self.consumer_channel, &self.consumer_tag)
            && channel.status().connected()
        {
            bounded(
                self.timeout,
                channel.basic_cancel(tag.as_str().into(), BasicCancelOptions::default()),
            )
            .await?;
        }
        self.consumer_tag = None;
        Ok(())
    }
    pub async fn flush(&self) -> Result<(), RabbitError> {
        if let Some(publisher) = &self.publisher {
            publisher.flush(self.timeout).await?;
        }
        Ok(())
    }
    pub async fn close(&mut self) -> Result<(), RabbitError> {
        self.session.stop();
        if let Some(task) = self.monitor.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = self.connecting.as_mut() {
            self.transport.stop();
            match timeout(self.timeout, &mut *task).await {
                Ok(Ok(Ok(connection))) => self.connection = Some(connection),
                _ => {
                    task.abort();
                }
            }
        }
        self.connecting = None;
        let result = if let Some(connection) = &self.connection
            && connection.status().connected()
        {
            bounded(self.timeout, connection.close(200, "OK".into())).await
        } else {
            Ok(())
        };
        self.transport.stop();
        self.consumer_tag = None;
        self.consumer_channel = None;
        self.publisher = None;
        self.declarations = None;
        self.connection = None;
        self.channels.lock().unwrap().clear();
        result
    }
}
impl Drop for RabbitMq {
    fn drop(&mut self) {
        self.session.stop();
        self.transport.stop();
        if let Some(task) = &self.monitor {
            task.abort();
        }
        if let Some(task) = &self.connecting {
            task.abort();
        }
    }
}
async fn bounded<T>(
    limit: Duration,
    future: impl std::future::Future<Output = lapin::Result<T>>,
) -> Result<T, RabbitError> {
    timeout(limit, future)
        .await
        .map_err(|_| RabbitError::Timeout)?
        .map_err(classify)
}
fn classify(error: lapin::Error) -> RabbitError {
    use lapin::protocol::{AMQPErrorKind, AMQPHardError, AMQPSoftError};
    match error.kind() {
        lapin::ErrorKind::ProtocolError(error) => match error.kind() {
            AMQPErrorKind::Soft(AMQPSoftError::PRECONDITIONFAILED) => RabbitError::Topology,
            AMQPErrorKind::Soft(AMQPSoftError::ACCESSREFUSED)
            | AMQPErrorKind::Hard(AMQPHardError::NOTALLOWED) => RabbitError::InvalidConfiguration,
            _ => RabbitError::Unavailable,
        },
        lapin::ErrorKind::AuthProviderError(_) => RabbitError::InvalidConfiguration,
        _ => RabbitError::Unavailable,
    }
}
