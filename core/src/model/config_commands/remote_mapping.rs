use serde::{Deserialize, Serialize};

use crate::model::mappings::Transformation;

use super::{default_false, no_entitymap, no_transformation};

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

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct EntityMapDecl {
    pub sql: String,
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

pub type ResolvedRemoteMappingsDeclaration = RemoteMappingsDeclaration;
