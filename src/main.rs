mod authorization;
mod startup;

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    startup::run(
        std::env::args().skip(1).collect(),
        authorization::check_startup,
    )
    .await
}
