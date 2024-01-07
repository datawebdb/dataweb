use query_runner_lib::{run, Result};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    run(None).await
}
