use crate::error::MeshError;
use crate::error::Result;

use diesel_async::{
    pooled_connection::bb8::{Pool, PooledConnection},
    AsyncPgConnection,
};

mod data;
mod entity;
mod mappings;
mod query;
mod relay;
mod user;
mod utils;

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
