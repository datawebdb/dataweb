#[cfg(feature = "datafusion")]
pub mod file_directory;
pub mod flight_sql;
#[cfg(feature = "trino")]
pub mod trino;

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::error::{MeshError, Result};

use crate::model::data_stores::options::file_directory::FileDirectorySource;
use crate::model::data_stores::options::{SourceOptions, SupportedObjectStore};
use crate::model::data_stores::{options::ConnectionOptions, DataConnection, DataSource};

#[cfg(feature = "os-aws")]
use object_store::aws::AmazonS3Builder;

#[cfg(feature = "os-azure")]
use object_store::azure::MicrosoftAzureBuilder;

#[cfg(feature = "os-gcp")]
use object_store::gcp::GoogleCloudStorageBuilder;

use object_store::{local::LocalFileSystem, ObjectStore};

use crate::model::query::Query;

#[cfg(feature = "datafusion")]
use self::file_directory::FileDirectoryRunner;
use self::flight_sql::FlightSQLRunner;
#[cfg(feature = "trino")]
use self::trino::TrinoRunner;

pub fn initialize_object_store(
    store_type: SupportedObjectStore,
    source: &FileDirectorySource,
) -> Result<Arc<dyn ObjectStore>> {
    let object_store: Arc<dyn ObjectStore> = match store_type {
        SupportedObjectStore::LocalFileSystem => {
            let local = match &source.prefix {
                Some(p) => LocalFileSystem::new_with_prefix(p)?,
                None => LocalFileSystem::new(),
            };
            Arc::new(local)
        }
        #[cfg(feature = "os-aws")]
        SupportedObjectStore::S3 => {
            let mut s3 = AmazonS3Builder::from_env();
            if let Some(bucket) = &source.bucket {
                s3 = s3.with_bucket_name(bucket);
            }
            if let Some(region) = &source.region {
                s3 = s3.with_region(region);
            }
            Arc::new(s3.build()?)
        }
        #[cfg(feature = "os-azure")]
        SupportedObjectStore::Azure => {
            let mut az = MicrosoftAzureBuilder::from_env();
            if let Some(bucket) = &source.bucket {
                az = az.with_container_name(bucket);
            }
            Arc::new(az.build()?)
        }
        #[cfg(feature = "os-gcp")]
        SupportedObjectStore::GCP => {
            let mut gcp = GoogleCloudStorageBuilder::from_env();
            if let Some(bucket) = &source.bucket {
                gcp = gcp.with_bucket_name(bucket);
            }
            Arc::new(gcp.build()?)
        }
    };
    Ok(object_store)
}

/// Attempts to establish a connection to a specific [DataConnection] which can query a specific
/// [DataSource].
pub async fn try_connect(
    con: DataConnection,
    source: DataSource,
) -> Result<Box<dyn QueryRunner + Send>> {
    match (con.connection_options, source.source_options) {
        #[cfg(feature = "datafusion")]
        (ConnectionOptions::FileDirectory(con_opts), SourceOptions::FileDirectory(source_opts)) => {
            Ok(Box::new(FileDirectoryRunner::try_from((
                con_opts,
                source_opts,
                source.name,
            ))?))
        }
        #[cfg(feature = "trino")]
        (ConnectionOptions::Trino(con_opts), SourceOptions::Trino(source_opts)) => {
            Ok(Box::new(TrinoRunner::try_from((con_opts, source_opts))?))
        }
        (ConnectionOptions::FlightSQL(con_opts), SourceOptions::FlightSQL(source_opts)) => Ok(
            Box::new(FlightSQLRunner::try_from((con_opts, source_opts))?),
        ),
        _ => Err(MeshError::InvalidQuery(format!(
            "Invalid or unsupported combination of \
                        DataConnection options and DataSource options: {}, {}",
            con.id, source.id
        ))),
    }
}

#[async_trait]
pub trait QueryRunner {
    /// Execute query, returning a stream of [RecordBatches][arrow_array::RecordBatch]
    async fn execute_stream(&mut self, query: Query) -> Result<SendableRecordBatchStream>;
}
