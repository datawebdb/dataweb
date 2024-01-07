use std::env;

use clap::Parser;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use mesh::crud::PgDb;

use mesh::error::Result;

use process::{
    process_entity_configs, process_local_data_configs, process_local_mapping_configs,
    process_relay_configs, process_remote_mapping_configs, process_user_configs,
};

mod models;
mod process;

#[derive(Parser)]
struct CliArgs {
    #[clap(short='e', long, default_value = None)]
    entity_configs: Option<std::path::PathBuf>,
    #[clap(short='d', long, default_value = None)]
    local_data_configs: Option<std::path::PathBuf>,
    #[clap(short='m', long, default_value = None)]
    local_mapping_configs: Option<std::path::PathBuf>,
    #[clap(short='r', long, default_value = None)]
    remote_relay_configs: Option<std::path::PathBuf>,
    #[clap(short='f', long, default_value = None)]
    remote_mapping_configs: Option<std::path::PathBuf>,
    #[clap(short='u', long, default_value = None)]
    user_mapping_configs: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = CliArgs::parse();
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(db_url);
    let pool = Pool::builder()
        .build(config)
        .await
        .expect("pool failed to start");

    let mut db = PgDb::try_from_pool(&pool).await?;

    if let Some(p) = args.entity_configs {
        process_entity_configs(&mut db, p).await?;
    }

    if let Some(p) = args.local_data_configs {
        process_local_data_configs(&mut db, p).await?;
    }

    if let Some(p) = args.local_mapping_configs {
        process_local_mapping_configs(&mut db, p).await?;
    }

    if let Some(p) = args.remote_relay_configs {
        process_relay_configs(&mut db, p).await?;
    }

    if let Some(p) = args.remote_mapping_configs {
        process_remote_mapping_configs(&mut db, p).await?;
    }

    if let Some(p) = args.user_mapping_configs {
        process_user_configs(&mut db, p).await?;
    }

    Ok(())
}
