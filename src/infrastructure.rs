//! Shared adapters and independently configured consumer runtimes.
use crate::{
    config::AppConfig,
    mongo::{MongoConnections, MongoRepositories},
    rabbitmq::{DeliveryPolicy, InboundDelivery, Publisher, RabbitError, RabbitMq},
    redis::{LeaseManager, MetadataCache, RedisConnections},
    runtime::{Failure, LifecycleAdapter, ShutdownStep, StartupStage},
};
use futures_util::{
    future::{BoxFuture, join_all},
    stream::{FuturesUnordered, StreamExt},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct WorkerServices {
    pub trade_publication: crate::trading::publication::TradePublication,
    pub static_low_balance_fallback_ratio_futures: f64,
    pub admission_client: Arc<crate::exchanges::bingx::client::BingxReadClient>,
    pub operation_timeout: Duration,
    pub telegram_sender: Option<Arc<crate::telegram::sender::TelegramSender>>,
    pub job_publication: crate::signals::publication::JobPublication,
    pub mongo: MongoRepositories,
    pub market: Arc<crate::exchanges::bingx::market_data::BingxMarketData>,
    pub locks: LeaseManager,
    pub metadata: MetadataCache,
    pub publisher: Publisher,
    pub delivery_policy: DeliveryPolicy,
}

/// The handler explicitly settles each delivery. It must keep all work in the
/// returned future so drain/abort can govern it. Use services.delivery_policy to
/// settle typed execution outcomes; only pre-claim failures may retry.
pub trait DeliveryHandler: Send + Sync + 'static {
    fn handle(
        &self,
        delivery: InboundDelivery,
        services: WorkerServices,
    ) -> BoxFuture<'static, Result<(), RabbitError>>;
}
pub struct Infrastructure {
    trade_publication: crate::trading::publication::TradePublication,
    static_low_balance_fallback_ratio_futures: f64,
    telegram_sender: Option<Arc<crate::telegram::sender::TelegramSender>>,
    job_publication: crate::signals::publication::JobPublication,
    market: Arc<crate::exchanges::bingx::market_data::BingxMarketData>,
    mongo: MongoConnections,
    redis: RedisConnections,
    consumers: Vec<ConsumerRuntime>,
    admission_client: Arc<crate::exchanges::bingx::client::BingxReadClient>,
    operation_timeout: Duration,
}

struct ConsumerRuntime {
    rabbit: RabbitMq,
    handler: Option<Arc<dyn DeliveryHandler>>,
    workers: Option<JoinHandle<Result<(), RabbitError>>>,
    workers_abort: CancellationToken,
    concurrency: usize,
    delivery_policy: DeliveryPolicy,
}
impl Infrastructure {
    /// Enable independently bounded consumer roles while sharing Mongo, Redis and HTTP clients.
    pub fn for_application(config: AppConfig) -> Result<Self, Failure> {
        let mut consumers = Vec::new();
        if config.telegram_intake_enabled {
            let mut rabbit = config.rabbitmq.clone();
            rabbit.input_queue = crate::contracts::rabbitmq::TELEGRAM_CHANNEL_QUEUE;
            consumers.push(ConsumerRuntime::new(
                rabbit,
                config.runtime.operation_timeout,
                Some(Arc::new(crate::telegram::TelegramHandler)),
            ));
        }
        {
            let mut rabbit = config.rabbitmq.clone();
            rabbit.input_queue = crate::contracts::rabbitmq::BINGX_FUTURES_QUEUE;
            rabbit.prefetch = config.client_trade_worker_prefetch;
            consumers.push(ConsumerRuntime::new(
                rabbit,
                config.runtime.operation_timeout,
                Some(Arc::new(crate::trading::job_handler::ClientTradeHandler)),
            ));
        }
        Self::with_consumers(config, consumers)
    }

    /// Select raw Telegram topology, retry policy, and handler together. The
    /// general constructor retains the configured BingX client-job boundary.
    pub fn for_telegram_intake(mut config: AppConfig) -> Result<Self, Failure> {
        config.rabbitmq.input_queue = crate::contracts::rabbitmq::TELEGRAM_CHANNEL_QUEUE;
        Self::new(config, Some(Arc::new(crate::telegram::TelegramHandler)))
    }

    /// Without a handler, opens infrastructure and declares queues but never consumes jobs.
    pub fn new(
        config: AppConfig,
        handler: Option<Arc<dyn DeliveryHandler>>,
    ) -> Result<Self, Failure> {
        let consumer = ConsumerRuntime::new(
            config.rabbitmq.clone(),
            config.runtime.operation_timeout,
            handler,
        );
        Self::with_consumers(config, vec![consumer])
    }

    fn with_consumers(config: AppConfig, consumers: Vec<ConsumerRuntime>) -> Result<Self, Failure> {
        let admission_client = crate::exchanges::bingx::client::BingxReadClient::new(
            &config.bingx_public_api_base_url,
            config.runtime.operation_timeout,
        )
        .map_err(|_| Failure::permanent("BINGX_CLIENT_CONFIGURATION"))?;
        let redis = RedisConnections::new(config.redis, &config.runtime).map_err(Failure::from)?;
        let market = if config.bingx_public_api_base_url == "https://open-api.bingx.com" {
            crate::exchanges::bingx::market_data::BingxMarketData::new(
                redis.cache(),
                config.runtime.operation_timeout,
            )
        } else {
            crate::exchanges::bingx::market_data::BingxMarketData::with_loopback_endpoint(
                redis.cache(),
                config.runtime.operation_timeout,
                &config.bingx_public_api_base_url,
            )
        }
        .map_err(|_| Failure::permanent("BINGX_CLIENT_CONFIGURATION"))?;
        let telegram_sender = config
            .telegram_bot_token
            .as_ref()
            .map(|token| {
                crate::telegram::sender::TelegramSender::new(
                    token.expose(),
                    &config.telegram_api_base_url,
                    config
                        .runtime
                        .operation_timeout
                        .min(std::time::Duration::from_secs(5)),
                )
                .map(Arc::new)
                .map_err(|_| Failure::permanent("TELEGRAM_SENDER_CONFIGURATION"))
            })
            .transpose()?;
        Ok(Self {
            trade_publication: crate::trading::publication::TradePublication::new(
                config.rabbitmq.retry_max_attempts.get(),
                config.rabbitmq.retry_delay,
            ),
            static_low_balance_fallback_ratio_futures: config
                .static_low_balance_fallback_ratio_futures,
            telegram_sender,
            job_publication: crate::signals::publication::JobPublication::new(
                config.rabbitmq.retry_max_attempts.get(),
                config.rabbitmq.retry_delay,
            ),
            market: Arc::new(market),
            mongo: MongoConnections::new(config.mongo, config.runtime.operation_timeout),
            redis,
            consumers,
            admission_client: Arc::new(admission_client),
            operation_timeout: config.runtime.operation_timeout,
        })
    }
    async fn start_workers(&mut self) -> Result<(), Failure> {
        self.mongo
            .verify_connections()
            .await
            .map_err(|_| Failure::restart("MONGO_UNAVAILABLE"))?;
        self.redis.check_required().map_err(Failure::from)?;
        for consumer in &self.consumers {
            consumer.rabbit.session.check().map_err(Failure::from)?;
        }
        let mongo = self.mongo.repositories().map_err(Failure::from)?;
        for consumer in &mut self.consumers {
            if consumer.handler.is_none() || consumer.workers.is_some() {
                continue;
            }
            let services = WorkerServices {
                trade_publication: self.trade_publication.clone(),
                static_low_balance_fallback_ratio_futures: self
                    .static_low_balance_fallback_ratio_futures,
                admission_client: self.admission_client.clone(),
                operation_timeout: self.operation_timeout,
                telegram_sender: self.telegram_sender.clone(),
                job_publication: self.job_publication.clone(),
                market: self.market.clone(),
                mongo: mongo.clone(),
                locks: self.redis.locks(),
                metadata: self.redis.cache(),
                publisher: consumer.rabbit.publisher().map_err(Failure::from)?,
                delivery_policy: consumer.delivery_policy.clone(),
            };
            consumer.start(services, &self.redis).await?;
        }
        Ok(())
    }
}

impl ConsumerRuntime {
    fn new(
        config: crate::config::RabbitMqConfig,
        timeout: Duration,
        handler: Option<Arc<dyn DeliveryHandler>>,
    ) -> Self {
        Self {
            concurrency: config.prefetch.get() as usize,
            delivery_policy: DeliveryPolicy::new(&config),
            rabbit: RabbitMq::new(config, timeout),
            handler,
            workers: None,
            workers_abort: CancellationToken::new(),
        }
    }

    async fn start(
        &mut self,
        services: WorkerServices,
        redis: &RedisConnections,
    ) -> Result<(), Failure> {
        let handler = self.handler.clone().expect("enabled consumer");
        let mut consumer = self.rabbit.start_consumer().await.map_err(Failure::from)?;
        // Registration may await the broker; recheck latched failures before
        // dispatching any buffered delivery to application code.
        redis.check_required().map_err(Failure::from)?;
        self.rabbit.session.check().map_err(Failure::from)?;
        let concurrency = self.concurrency;
        let session = self.rabbit.session.clone();
        self.workers_abort = CancellationToken::new();
        let abort = self.workers_abort.clone();
        self.workers = Some(tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let mut result=async {
                loop {
                    tokio::select! { biased;
                        _=abort.cancelled()=>break,
                        _=session.intake.cancelled()=>break,
                        result=tasks.join_next(),if !tasks.is_empty()=>{ result.unwrap().map_err(|_|RabbitError::DeliveryAbandoned)??; },
                        delivery=consumer.next(),if tasks.len()<concurrency=>match delivery? {
                            Some(delivery)=> {
                                let handler=handler.clone();let services=services.clone();
                                tasks.spawn(async move {handler.handle(delivery,services).await});
                            },
                            None=>break,
                        },
                    }
                }
                Ok(())
            }.await;
            let mut aborting = result.is_err() || abort.is_cancelled();
            if aborting {
                tasks.abort_all();
            }
            if let Err(error) = result {
                session.fail(error);
            }
            // Join cancelled children as well as successful ones before releasing services.
            while !tasks.is_empty() {
                tokio::select! { biased;
                    _=abort.cancelled(),if !aborting=>{tasks.abort_all();aborting=true;},
                    child=tasks.join_next()=> {
                        let child=match child.unwrap() {
                            Ok(result)=>result,
                            Err(error) if aborting && error.is_cancelled()=>Ok(()),
                            Err(_)=>Err(RabbitError::DeliveryAbandoned),
                        };
                        if let Err(error)=child {
                            if result.is_ok() {result=Err(error);session.fail(error);}
                            tasks.abort_all();aborting=true;
                        }
                    }
                }
            }
            result
        }));
        Ok(())
    }
}
impl LifecycleAdapter for Infrastructure {
    async fn initialize(&mut self, stage: StartupStage) -> Result<(), Failure> {
        match stage {
            StartupStage::MongoConnections => self.mongo.connect().await.map_err(Into::into),
            StartupStage::MongoIndexes => self.mongo.initialize_indexes().await.map_err(Into::into),
            StartupStage::RedisRequired => {
                self.redis.connect_required().await.map_err(|e| match e {
                    crate::redis::RedisError::InvalidConfiguration => {
                        Failure::permanent("REDIS_INVALID_CONFIGURATION")
                    }
                    _ => Failure::retryable("REDIS_UNAVAILABLE"),
                })
            }
            StartupStage::RabbitDeclarations => all_consumers(
                join_all(
                    self.consumers
                        .iter_mut()
                        .map(|consumer| consumer.rabbit.connect_and_declare()),
                )
                .await,
            ),
            StartupStage::RabbitPublisher => all_consumers(
                join_all(
                    self.consumers
                        .iter_mut()
                        .map(|consumer| consumer.rabbit.initialize_publisher()),
                )
                .await,
            ),
            StartupStage::Consumers => self.start_workers().await,
        }
    }
    fn quiesce(&mut self) {
        for consumer in &self.consumers {
            consumer.rabbit.quiesce();
        }
        self.redis.quiesce();
    }
    fn abort_in_flight(&mut self) {
        for consumer in &self.consumers {
            consumer.workers_abort.cancel();
        }
    }
    async fn shutdown(&mut self, step: ShutdownStep) -> Result<(), Failure> {
        match step {
            ShutdownStep::Consumers => all_consumers(
                join_all(
                    self.consumers
                        .iter_mut()
                        .map(|consumer| consumer.rabbit.stop_consumers()),
                )
                .await,
            ),
            ShutdownStep::Drain => {
                join_all(
                    self.consumers
                        .iter_mut()
                        .map(|consumer| consumer.join_workers()),
                )
                .await;
                Ok(())
            }
            ShutdownStep::FlushPublisher => {
                join_all(self.consumers.iter_mut().map(|consumer| async move {
                    if consumer.workers_abort.is_cancelled() {
                        consumer.join_workers().await;
                    }
                }))
                .await;
                all_consumers(
                    join_all(
                        self.consumers
                            .iter()
                            .map(|consumer| consumer.rabbit.flush()),
                    )
                    .await,
                )
            }
            ShutdownStep::BackgroundTasks => {
                for consumer in &self.consumers {
                    consumer.workers_abort.cancel();
                }
                join_all(
                    self.consumers
                        .iter_mut()
                        .map(|consumer| consumer.join_workers()),
                )
                .await;
                Ok(())
            }
            ShutdownStep::RabbitMq => all_consumers(
                join_all(
                    self.consumers
                        .iter_mut()
                        .map(|consumer| consumer.rabbit.close()),
                )
                .await,
            ),
            ShutdownStep::Redis => {
                self.redis.close().await;
                Ok(())
            }
            ShutdownStep::Mongo => self.mongo.close().await.map_err(Into::into),
        }
    }
    async fn wait_for_failure(&mut self) -> Failure {
        let mongo_failure = async {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                if self.mongo.verify_connections().await.is_err() {
                    break Failure::restart("MONGO_UNAVAILABLE");
                }
            }
        };
        let rabbit_failure = async {
            let mut failures: FuturesUnordered<_> = self
                .consumers
                .iter()
                .map(|consumer| consumer.rabbit.wait_for_failure())
                .collect();
            match failures.next().await {
                Some(error) => Failure::from(error),
                None => std::future::pending().await,
            }
        };
        tokio::select! { biased;
            error=rabbit_failure=>error,
            error=self.redis.wait_for_failure()=>error.into(),
            error=mongo_failure=>error,
        }
    }
}
fn all_consumers(results: Vec<Result<(), RabbitError>>) -> Result<(), Failure> {
    results
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map(|_| ())
        .map_err(Into::into)
}

impl ConsumerRuntime {
    async fn join_workers(&mut self) {
        if let Some(task) = self.workers.as_mut() {
            let _ = task.await;
        }
        self.workers = None;
    }
}

impl Drop for Infrastructure {
    fn drop(&mut self) {
        // Stop workers before Rust drops the shared Mongo and Redis adapters.
        for consumer in &self.consumers {
            consumer.workers_abort.cancel();
            if let Some(task) = &consumer.workers {
                task.abort();
            }
        }
    }
}

impl Drop for ConsumerRuntime {
    fn drop(&mut self) {
        self.workers_abort.cancel();
        if let Some(task) = &self.workers {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(intake: bool) -> AppConfig {
        let mut config = AppConfig::from_lookup(|key| {
            Some(
                match key {
                    "RABBIT_MQ" => "amqp://localhost:5672/%2f",
                    "MONGO_PATH" => "mongodb://localhost:27017/bot",
                    "TRADE_STATION_MONGO_PATH" => "mongodb://localhost:27017/trades",
                    "ACCOUNT_VALIDATOR_MONGO_PATH" => "mongodb://localhost:27017/accounts",
                    "REDIS" => "localhost",
                    "CONSUMER_PREFETCH" => "7",
                    "CLIENT_TRADE_WORKER_PREFETCH" => "3",
                    _ => return None,
                }
                .into(),
            )
        })
        .unwrap();
        config.telegram_intake_enabled = intake;
        config
    }

    #[tokio::test]
    async fn application_always_registers_worker_with_optional_intake() {
        for (intake, expected) in [(false, vec![3]), (true, vec![7, 3])] {
            let mut infrastructure = Infrastructure::for_application(config(intake)).unwrap();
            assert_eq!(
                infrastructure
                    .consumers
                    .iter()
                    .map(|role| role.concurrency)
                    .collect::<Vec<_>>(),
                expected
            );
            assert!(
                infrastructure
                    .consumers
                    .iter()
                    .all(|role| role.handler.is_some())
            );
            infrastructure.quiesce();
            infrastructure.abort_in_flight();
            assert!(
                infrastructure.consumers.iter().all(|role| role
                    .rabbit
                    .session
                    .intake
                    .is_cancelled()
                    && role.workers_abort.is_cancelled())
            );
        }
    }

    #[tokio::test]
    async fn explicit_constructor_keeps_one_configured_role() {
        let infrastructure = Infrastructure::new(config(true), None).unwrap();
        assert_eq!(infrastructure.consumers.len(), 1);
        assert_eq!(infrastructure.consumers[0].concurrency, 7);
        assert!(infrastructure.consumers[0].handler.is_none());
    }

    #[tokio::test]
    async fn dropping_infrastructure_cancels_every_worker_role() {
        let infrastructure = Infrastructure::for_application(config(true)).unwrap();
        let cancellations: Vec<_> = infrastructure
            .consumers
            .iter()
            .map(|consumer| consumer.workers_abort.clone())
            .collect();
        drop(infrastructure);
        assert!(cancellations.iter().all(CancellationToken::is_cancelled));
    }

    #[tokio::test]
    async fn drain_joins_every_role_before_releasing_worker_handles() {
        let mut infrastructure = Infrastructure::for_application(config(true)).unwrap();
        let completed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        for consumer in &mut infrastructure.consumers {
            let cancelled = consumer.workers_abort.clone();
            let completed = completed.clone();
            consumer.workers = Some(tokio::spawn(async move {
                cancelled.cancelled().await;
                completed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }));
        }
        infrastructure.abort_in_flight();
        infrastructure.shutdown(ShutdownStep::Drain).await.unwrap();
        assert_eq!(completed.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(
            infrastructure
                .consumers
                .iter()
                .all(|role| role.workers.is_none())
        );
    }
}
