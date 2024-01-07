use serde::{Deserialize, Serialize};

use super::{SourceFileType, SupportedObjectStore};

/// Represents files (such as parquet or CSV) stored in an ObjectStore.
/// Uses DataFusion's ListingTable table provider to query.
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileDirectorySource {
    pub bucket: Option<String>,
    pub region: Option<String>,
    pub prefix: Option<String>,
    pub file_type: SourceFileType,
}

/// Information needed to identify and connect to files in an ObjectStore
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileDirectoryConnection {
    pub object_store_type: SupportedObjectStore,
    pub url: String,
}
