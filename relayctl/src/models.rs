use mesh::model::{
    data_stores::options::{ConnectionOptions, SourceOptions},
    query::SubstitutionBlocks,
};

use mesh::model::mappings::Transformation;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct InformationDeclaration {
    pub(crate) name: String,
    pub(crate) arrow_dtype: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct EntityDeclaration {
    pub(crate) name: String,
    pub(crate) information: Vec<InformationDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct DataConnectionsDeclaration {
    pub(crate) name: String,
    pub(crate) connection_options: ConnectionOptions,
    pub(crate) data_sources: Vec<DataSourcesDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct DataSourcesDeclaration {
    pub(crate) name: String,
    pub(crate) source_sql: String,
    pub(crate) source_options: SourceOptions,
    pub(crate) fields: Vec<DataFieldsDeclaration>,
    #[serde(default = "empty_permission")]
    pub(crate) default_permission: DefaultPermissionDeclaration,
}

fn empty_permission() -> DefaultPermissionDeclaration {
    DefaultPermissionDeclaration {
        allowed_columns: vec![],
        allowed_rows: "false".to_string(),
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct DefaultPermissionDeclaration {
    pub(crate) allowed_columns: Vec<String>,
    pub(crate) allowed_rows: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct DataFieldsDeclaration {
    pub(crate) name: String,
    pub(crate) path: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct LocalMappingDeclaration {
    pub(crate) entity_name: String,
    pub(crate) mappings: Vec<DataConnectionMappingDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct DataConnectionMappingDeclaration {
    pub(crate) data_con_name: String,
    pub(crate) source_mappings: Vec<DataSourceMappingsDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct DataSourceMappingsDeclaration {
    pub(crate) data_source_name: String,
    pub(crate) field_mappings: Vec<DataFieldMappingDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct DataFieldMappingDeclaration {
    pub(crate) info: String,
    pub(crate) field: String,
    #[serde(default = "no_transformation")]
    pub(crate) transformation: Transformation,
}

fn no_transformation() -> Transformation {
    Transformation {
        other_to_local_info: "{value}".to_string(),
        local_info_to_other: "{value}".to_string(),
        replace_from: "{value}".to_string(),
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct RemoteMappingsDeclaration {
    pub(crate) entity_name: String,
    pub(crate) mappings: Vec<PeerRelayMappingsDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct PeerRelayMappingsDeclaration {
    pub(crate) relay_name: String,
    pub(crate) remote_entity_name: String,
    #[serde(default = "no_entitymap")]
    pub(crate) entity_map: Option<EntityMapDecl>,
    pub(crate) relay_mappings: Vec<RemoteInfoMappingsDeclaration>,
}

fn no_entitymap() -> Option<EntityMapDecl> {
    None
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct EntityMapDecl {
    pub(crate) sql: String,
    pub(crate) substitution_blocks: SubstitutionBlocks,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct RemoteInfoMappingsDeclaration {
    pub(crate) local_info: String,
    pub(crate) info_mapped_name: String,
    #[serde(default = "default_false")]
    pub(crate) literal_derived_field: bool,
    #[serde(default = "no_transformation")]
    pub(crate) transformation: Transformation,
}

fn default_false() -> bool {
    false
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct PeerRelayDeclaration {
    pub(crate) name: String,
    pub(crate) rest_endpoint: String,
    pub(crate) flight_endpoint: String,
    pub(crate) x509_cert_file: String,
    #[serde(default = "empty_permissions")]
    pub(crate) permissions: Option<Vec<PermissionsDecl>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct UserDeclaration {
    pub(crate) x509_cert_file: String,
    #[serde(default = "empty_permissions")]
    pub(crate) permissions: Option<Vec<PermissionsDecl>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct PermissionsDecl {
    pub(crate) data_con_name: String,
    pub(crate) source_permissions: Vec<SourcePermissionDecl>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub(crate) struct SourcePermissionDecl {
    pub(crate) data_source_name: String,
    pub(crate) allowed_columns: Vec<String>,
    pub(crate) allowed_rows: String,
}

fn empty_permissions() -> Option<Vec<PermissionsDecl>> {
    None
}
