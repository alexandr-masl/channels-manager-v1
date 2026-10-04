//! Composition of the concrete adapters. Only an explicitly supplied handler enables intake.
use crate::{
    config::AppConfig,
    mongo::{MongoConnections, MongoRepositories},
    rabbitmq::{DeliveryPolicy, InboundDelivery, Publisher, RabbitError, RabbitMq},
    redis::{LeaseManager, MetadataCache, RedisConnections},
    runtime::{Failure, LifecycleAdapter, ShutdownStep, StartupStage},
};
use futures_util::future::BoxFuture;
use std::sync::Arc;
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct WorkerServices {
    pub mongo: MongoRepositories,
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
    mongo: MongoConnections,
    redis: RedisConnections,
    rabbit: RabbitMq,
    handler: Option<Arc<dyn DeliveryHandler>>,
    workers: Option<JoinHandle<Result<(), RabbitError>>>,
    workers_abort: CancellationToken,
    concurrency: usize,
    delivery_policy: DeliveryPolicy,
}
impl Infrastructure {
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
        Ok(Self {
            mongo: MongoConnections::new(config.mongo, config.runtime.operation_timeout),
            redis: RedisConnections::new(config.redis, &config.runtime).map_err(Failure::from)?,
            concurrency: config.rabbitmq.prefetch.get() as usize,
            delivery_policy: DeliveryPolicy::new(&config.rabbitmq),
            rabbit: RabbitMq::new(config.rabbitmq, config.runtime.operation_timeout),
            handler,
            workers: None,
            workers_abort: CancellationToken::new(),
        })
    }
    async fn start_workers(&mut self) -> Result<(), Failure> {
        self.mongo
            .verify_connections()
            .await
            .map_err(|_| Failure::restart("MONGO_UNAVAILABLE"))?;
        self.redis.check_required().map_err(Failure::from)?;
        self.rabbit.session.check().map_err(Failure::from)?;
        let Some(handler) = self.handler.clone() else {
            return Ok(());
        };
        if self.workers.is_some() {
            return Ok(());
        }
        let services = WorkerServices {
            mongo: self.mongo.repositories().map_err(Failure::from)?,
            locks: self.redis.locks(),
            metadata: self.redis.cache(),
            publisher: self.rabbit.publisher().map_err(Failure::from)?,
            delivery_policy: self.delivery_policy.clone(),
        };
        let mut consumer = self.rabbit.start_consumer().await.map_err(Failure::from)?;
        // Registration may await the broker; recheck latched failures before
        // dispatching any buffered delivery to application code.
        self.redis.check_required().map_err(Failure::from)?;
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
            StartupStage::RabbitDeclarations => {
                self.rabbit.connect_and_declare().await.map_err(Into::into)
            }
            StartupStage::RabbitPublisher => {
                self.rabbit.initialize_publisher().await.map_err(Into::into)
            }
            StartupStage::Consumers => self.start_workers().await,
        }
    }
    fn quiesce(&mut self) {
        self.rabbit.quiesce();
        self.redis.quiesce();
    }
    fn abort_in_flight(&mut self) {
        self.workers_abort.cancel();
    }
    async fn shutdown(&mut self, step: ShutdownStep) -> Result<(), Failure> {
        match step {
            ShutdownStep::Consumers => self.rabbit.stop_consumers().await.map_err(Into::into),
            ShutdownStep::Drain => {
                if let Some(task) = self.workers.as_mut() {
                    // Worker failures are already latched. Joining ensures every child is dropped.
                    let _ = task.await;
                }
                self.workers = None;
                Ok(())
            }
            ShutdownStep::FlushPublisher => {
                if self.workers_abort.is_cancelled() {
                    if let Some(task) = self.workers.as_mut() {
                        let _ = task.await;
                    }
                    self.workers = None;
                }
                self.rabbit.flush().await.map_err(Into::into)
            }
            ShutdownStep::BackgroundTasks => {
                self.workers_abort.cancel();
                if let Some(task) = self.workers.as_mut() {
                    let _ = task.await;
                }
                self.workers = None;
                Ok(())
            }
            ShutdownStep::RabbitMq => self.rabbit.close().await.map_err(Into::into),
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
        tokio::select! { biased;
            error=self.rabbit.wait_for_failure()=>error.into(),
            error=self.redis.wait_for_failure()=>error.into(),
            error=mongo_failure=>error,
        }
    }
}
impl Drop for Infrastructure {
    fn drop(&mut self) {
        self.workers_abort.cancel();
        if let Some(task) = &self.workers {
            task.abort();
        }
    }
}
