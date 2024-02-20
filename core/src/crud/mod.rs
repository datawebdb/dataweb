use std::time::Duration;

use crate::error::MeshError;
use crate::error::Result;

use diesel::Connection;
use diesel::PgConnection;
use diesel_async::{
    pooled_connection::bb8::{Pool, PooledConnection},
    AsyncPgConnection,
};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use tracing::error;

mod data;
mod entity;
mod mappings;
mod query;
mod relay;
mod user;
mod utils;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

/// Bootstraps database with diesel migrations uses embedded code. Continues to retry on error, logging
/// the error cause, and sleeping for a time. Intermittent connectivity errors on startup are expected while
/// Postgres is initializing. Panics if the migrations fail for any reason other inability to connect.
pub fn run_migrations(db_url: &str) {
    let mut con;
    loop {
        let maybe_connected = PgConnection::establish(db_url);
        match maybe_connected {
            Ok(c) => {
                con = c;
                break;
            }
            Err(e) => {
                error!("Failed connecting to postgres with error: {e}... retrying migrations in 5 seconds");
                std::thread::sleep(Duration::from_secs(5))
            }
        }
    }

    con.run_pending_migrations(MIGRATIONS)
        .unwrap_or_else(|e| panic!("Error running migrations for {} with error {e}", db_url));
}

/// Holds a [AsyncPgConnection] borrowed from a [Pool] and implements CRUD operations
pub struct PgDb<'a> {
    con: PooledConnection<'a, AsyncPgConnection>,
}

// See sub modules for additional method impls
impl<'a> PgDb<'a> {
    pub async fn try_from_pool(pool: &'a Pool<AsyncPgConnection>) -> Result<PgDb<'a>> {
        let con = pool.get().await.map_err(|_| {
            MeshError::DbError("Error connecting to the database connection pool!".into())
        })?;
        Ok(Self { con })
    }
}
