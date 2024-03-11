use serde::{Deserialize, Serialize};

use crate::model::mappings::Transformation;

use super::no_transformation;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RemoteMappingsDeclaration {
    pub entity_name: String,
    pub mappings: Vec<PeerRelayMappingsDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PeerRelayMappingsDeclaration {
    pub relay_name: String,
    pub remote_entity_name: String,
    #[serde(default = "no_entity_map_sql")]
    pub sql: Option<String>,
    pub relay_mappings: Vec<RemoteInfoMappingsDeclaration>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RemoteInfoMappingsDeclaration {
    pub local_info: String,
    pub info_mapped_name: String,
    #[serde(default = "no_transformation")]
    pub transformation: Transformation,
}

fn no_entity_map_sql() -> Option<String> {
    None
}

pub type ResolvedRemoteMappingsDeclaration = RemoteMappingsDeclaration;
