use channels_manager_v1::{
    config::AppConfig,
    contracts::rabbitmq::TELEGRAM_CHANNEL_QUEUE,
    infrastructure::{DeliveryHandler, Infrastructure, WorkerServices},
    rabbitmq::{InboundDelivery, RabbitError},
    runtime::{Lifecycle, run_until_signal},
};
use std::{process::ExitCode, sync::Arc};

struct LogTelegramMessage;
impl DeliveryHandler for LogTelegramMessage {
    fn handle(
        &self,
        delivery: InboundDelivery,
        _services: WorkerServices,
    ) -> futures_util::future::BoxFuture<'static, Result<(), RabbitError>> {
        Box::pin(async move {
            println!(
                "Incoming {TELEGRAM_CHANNEL_QUEUE}: {:?}",
                String::from_utf8_lossy(delivery.body())
            );
            delivery.ack().await
        })
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let check = match args.as_slice() {
        [] => false,
        [flag] if flag == "--check-config" => true,
        _ => {
            eprintln!("Usage: channels-manager-v1 [--check-config]");
            return ExitCode::FAILURE;
        }
    };
    let mut config = match AppConfig::from_env() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("Configuration error: {error}");
            return ExitCode::FAILURE;
        }
    };
    if check {
        println!("Configuration valid.");
        return ExitCode::SUCCESS;
    }
    config.rabbitmq.input_queue = TELEGRAM_CHANNEL_QUEUE;
    let runtime = config.runtime.clone();
    let adapter = match Infrastructure::new(config, Some(Arc::new(LogTelegramMessage))) {
        Ok(adapter) => adapter,
        Err(error) => {
            eprintln!("Infrastructure error: {}", error.code);
            return ExitCode::FAILURE;
        }
    };
    let lifecycle = match Lifecycle::new(adapter, runtime) {
        Ok(lifecycle) => lifecycle,
        Err(error) => {
            eprintln!("Runtime error: {error}");
            return ExitCode::FAILURE;
        }
    };
    println!(
        "Starting infrastructure; incoming {TELEGRAM_CHANNEL_QUEUE} messages will be logged and acknowledged."
    );
    match run_until_signal(lifecycle).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
