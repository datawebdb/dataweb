use serde::{Deserialize, Serialize};

use crate::model::data_stores::options::{ConnectionOptions, SourceOptions};

use super::{empty_permission, DefaultPermissionDeclaration};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataConnectionsDeclaration {
    pub name: String,
    pub connection_options: ConnectionOptions,
    pub data_sources: Vec<DataSourcesDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataSourcesDeclaration {
    pub name: String,
    pub source_sql: String,
    pub source_options: SourceOptions,
    pub fields: Vec<DataFieldsDeclaration>,
    #[serde(default = "empty_permission")]
    pub default_permission: DefaultPermissionDeclaration,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataFieldsDeclaration {
    pub name: String,
    pub path: String,
}

pub type ResolvedDataConnectionsDeclaration = DataConnectionsDeclaration;
