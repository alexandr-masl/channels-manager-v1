use channels_manager_v1::config::AppConfig;
use std::process::ExitCode;

fn main() -> ExitCode {
    match AppConfig::from_env() {
        Ok(_config) => {
            println!("Configuration valid. Infrastructure connections are not started in stage 1.");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("Configuration error: {error}");
            ExitCode::FAILURE
        }
    }
}
