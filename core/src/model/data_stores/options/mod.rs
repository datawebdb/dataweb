use diesel_as_jsonb::AsJsonb;
use serde::{Deserialize, Serialize};

use diesel::{AsExpression, FromSqlRow};

use crate::error::MeshError;

#[cfg(feature = "datafusion")]
use self::file_directory::{FileDirectoryConnection, FileDirectorySource};
use self::flight_sql::{FlightSQLSource, FlightSqlConnection};
#[cfg(feature = "trino")]
use self::trino::{TrinoConnection, TrinoSource};

pub mod file_directory;
pub mod flight_sql;
pub mod trino;

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceFileType {
    CSV,
    JSON,
    Parquet,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SupportedObjectStore {
    LocalFileSystem,
    #[cfg(feature = "os-aws")]
    S3,
    #[cfg(feature = "os-azure")]
    Azure,
    #[cfg(feature = "os-gcp")]
    GCP,
}

impl TryFrom<String> for SupportedObjectStore {
    type Error = MeshError;
    fn try_from(value: String) -> Result<Self, MeshError> {
        match value.as_str() {
            "LocalFileSystem" => Ok(Self::LocalFileSystem),
            #[cfg(feature = "os-aws")]
            "S3" => Ok(Self::S3),
            #[cfg(feature = "os-azure")]
            "Azure" => Ok(Self::Azure),
            #[cfg(feature = "os-gcp")]
            "GCP" => Ok(Self::GCP),
            _ => Err(MeshError::SerDe(format!(
                "Invalid Object Store variant specified: {value}. \
            Valid values are LocalFileSystem, S3, Azure, or GCP"
            ))),
        }
    }
}

/// The suported [DataConnection][crate::model::data_stores::DataConnection] backend stores and contains
/// the information needed to connect to each store.
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, AsJsonb)]
pub enum ConnectionOptions {
    /// Represents a collection of files in any ObjectStore compatible interface, such as S3
    /// azure blob, MinIO, or a local file system / network drive.
    #[cfg(feature = "datafusion")]
    FileDirectory(FileDirectoryConnection),
    #[cfg(feature = "trino")]
    Trino(TrinoConnection),
    FlightSQL(FlightSqlConnection),
}

/// The suported [DataSource][crate::model::data_stores::DataSource] backend stores and contains
/// the information needed to query a specific dataset within
/// a [DataConnection][crate::model::data_stores::DataConnection].
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize, AsJsonb)]
pub enum SourceOptions {
    /// Represents a collection of files in any ObjectStore compatible interface, such as S3
    /// azure blob, MinIO, or a local file system / network drive.
    #[cfg(feature = "datafusion")]
    FileDirectory(FileDirectorySource),
    #[cfg(feature = "trino")]
    Trino(TrinoSource),
    FlightSQL(FlightSQLSource),
}
