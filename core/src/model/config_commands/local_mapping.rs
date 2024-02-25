use crate::model::config_commands::no_transformation;
use crate::model::mappings::Transformation;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct LocalMappingDeclaration {
    pub entity_name: String,
    pub mappings: Vec<DataConnectionMappingDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataConnectionMappingDeclaration {
    pub data_con_name: String,
    pub source_mappings: Vec<DataSourceMappingsDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataSourceMappingsDeclaration {
    pub data_source_name: String,
    pub field_mappings: Vec<DataFieldMappingDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataFieldMappingDeclaration {
    pub info: String,
    pub field: String,
    #[serde(default = "no_transformation")]
    pub transformation: Transformation,
}

pub type ResolvedLocalMappingDeclaration = LocalMappingDeclaration;
