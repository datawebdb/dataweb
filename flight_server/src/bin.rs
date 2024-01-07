use flight_server_lib::run;
use mesh::error::MeshError;

#[tokio::main]
async fn main() -> Result<(), MeshError> {
    tracing_subscriber::fmt::init();
    run().await
}
