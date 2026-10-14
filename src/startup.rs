use channels_manager_v1::{
    config::AppConfig,
    infrastructure::Infrastructure,
    runtime::{Lifecycle, run_until_signal},
};
use std::process::ExitCode;

const AUTHORIZATION_NOTICE: &str = "Authorization required. To run this application, obtain permission from the repository owner, Alexandr Maslov. Repository or container access alone does not authorize operation.";

pub(crate) async fn run(
    args: Vec<String>,
    check_authorization: impl FnOnce() -> Result<(), crate::authorization::AuthorizationError>,
) -> ExitCode {
    let check = match args.as_slice() {
        [] => false,
        [flag] if flag == "--check-config" => true,
        _ => {
            eprintln!("Usage: channels-manager-v1 [--check-config]");
            return ExitCode::FAILURE;
        }
    };
    if !check && let Err(error) = check_authorization() {
        eprintln!("Authorization error: {}", error.code());
        eprintln!("{AUTHORIZATION_NOTICE}");
        return ExitCode::from(78);
    }
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
    println!(
        "Telegram acceptance replies: {}",
        if config.telegram_bot_token.is_some() {
            "enabled"
        } else {
            "disabled (SATOSHI_TG_TOKEN unset)"
        }
    );
    println!(
        "Consumer roles: Telegram intake={}, BingX trade worker=enabled; destination={}",
        config.telegram_intake_enabled, config.rabbitmq.output_queue
    );
    let runtime = config.runtime.clone();
    let adapter = match Infrastructure::for_application(config) {
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
        "Starting infrastructure; accepted signals publish client jobs and admitted trades publish to Trading Station."
    );
    match run_until_signal(lifecycle).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
