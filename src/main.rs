use channels_manager_v1::{
    config::AppConfig,
    infrastructure::Infrastructure,
    runtime::{Lifecycle, run_until_signal},
};
use std::process::ExitCode;

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
    let config = match AppConfig::from_env() {
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
    let runtime = config.runtime.clone();
    let adapter = match Infrastructure::new(config, None) {
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
    println!("Starting infrastructure. Job consumption requires a delivery handler.");
    match run_until_signal(lifecycle).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
