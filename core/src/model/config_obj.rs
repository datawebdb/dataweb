use std::collections::HashMap;

use crate::model::{
    data_stores::options::{ConnectionOptions, SourceOptions},
    query::SubstitutionBlocks,
};

use crate::model::mappings::Transformation;
use serde::{Deserialize, Serialize};

/// These are declarative, non relational representations of objects that configure
/// the behavior of the Relay. Admins maintain these definitions and the Relay is responsible
/// for translating them to the relational model equivalents and updating the database to be in
/// line with the declared state.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ConfigObject {
    EntityConfig(EntityDeclaration),
    LocalDataConfig(DataConnectionsDeclaration),
    LocalMappingConfig(LocalMappingDeclaration),
    PeerRelayConfig(PeerRelayDeclaration),
    RemoteMappingConfig(RemoteMappingsDeclaration),
    UserConfig(UserDeclaration),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct InformationDeclaration {
    pub name: String,
    pub arrow_dtype: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct EntityDeclaration {
    pub name: String,
    pub information: Vec<InformationDeclaration>,
}

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

fn empty_permission() -> DefaultPermissionDeclaration {
    DefaultPermissionDeclaration {
        allowed_columns: vec![],
        allowed_rows: "false".to_string(),
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DefaultPermissionDeclaration {
    pub allowed_columns: Vec<String>,
    pub allowed_rows: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DataFieldsDeclaration {
    pub name: String,
    pub path: String,
}

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

fn no_transformation() -> Transformation {
    Transformation {
        other_to_local_info: "{value}".to_string(),
        local_info_to_other: "{value}".to_string(),
        replace_from: "{value}".to_string(),
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RemoteMappingsDeclaration {
    pub entity_name: String,
    pub mappings: Vec<PeerRelayMappingsDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PeerRelayMappingsDeclaration {
    pub relay_name: String,
    pub remote_entity_name: String,
    #[serde(default = "no_entitymap")]
    pub entity_map: Option<EntityMapDecl>,
    pub relay_mappings: Vec<RemoteInfoMappingsDeclaration>,
}

fn no_entitymap() -> Option<EntityMapDecl> {
    None
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct EntityMapDecl {
    pub sql: String,
    pub substitution_blocks: SubstitutionBlocks,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RemoteInfoMappingsDeclaration {
    pub local_info: String,
    pub info_mapped_name: String,
    #[serde(default = "default_false")]
    pub literal_derived_field: bool,
    #[serde(default = "no_transformation")]
    pub transformation: Transformation,
}

fn default_false() -> bool {
    false
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PeerRelayDeclaration {
    pub name: String,
    pub rest_endpoint: String,
    pub flight_endpoint: String,
    pub x509_cert: Vec<u8>,
    #[serde(default = "empty_permissions")]
    pub permissions: Option<Vec<PermissionsDecl>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UserDeclaration {
    pub x509_cert: Vec<u8>,
    pub attributes: HashMap<String, String>,
    #[serde(default = "empty_permissions")]
    pub permissions: Option<Vec<PermissionsDecl>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PermissionsDecl {
    pub data_con_name: String,
    pub source_permissions: Vec<SourcePermissionDecl>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SourcePermissionDecl {
    pub data_source_name: String,
    pub allowed_columns: Vec<String>,
    pub allowed_rows: String,
}

fn empty_permissions() -> Option<Vec<PermissionsDecl>> {
    None
}
