use serde::{Deserialize, Serialize};

use self::{
    entity::{EntityDeclaration, ResolvedEntityDeclaration},
    local_data::{DataConnectionsDeclaration, ResolvedDataConnectionsDeclaration},
    local_mapping::{LocalMappingDeclaration, ResolvedLocalMappingDeclaration},
    relay::{PeerRelayDeclaration, ResolvedPeerRelayDeclaration},
    remote_mapping::{EntityMapDecl, RemoteMappingsDeclaration, ResolvedRemoteMappingsDeclaration},
    user::{PermissionsDecl, ResolvedUserDeclaration, UserDeclaration},
};

use super::mappings::Transformation;

pub mod entity;
pub mod local_data;
pub mod local_mapping;
pub mod relay;
pub mod remote_mapping;
pub mod user;

/// Describes a desired state for a declared [ConfigObject].
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ResolvedConfigCommand {
    pub api_version: String,
    pub config_object: ResolvedConfigObject,
}

/// Describes a desired state for a declared [ConfigObject].
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ConfigCommand {
    pub api_version: String,
    pub config_object: ConfigObject,
}

/// These are declarative, non relational representations of objects that configure
/// the behavior of the Relay. Admins maintain these definitions and the Relay is responsible
/// for translating them to the relational model equivalents and updating the database to be in
/// line with the declared state.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "kind", content = "spec")]
pub enum ResolvedConfigObject {
    Entity(ResolvedEntityDeclaration),
    LocalData(ResolvedDataConnectionsDeclaration),
    LocalMapping(ResolvedLocalMappingDeclaration),
    PeerRelay(ResolvedPeerRelayDeclaration),
    RemoteMapping(ResolvedRemoteMappingsDeclaration),
    User(ResolvedUserDeclaration),
}

/// These are declarative, non relational representations of objects that configure
/// the behavior of the Relay. Admins maintain these definitions and the Relay is responsible
/// for translating them to the relational model equivalents and updating the database to be in
/// line with the declared state.
#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "kind", content = "spec")]
pub enum ConfigObject {
    Entity(EntityDeclaration),
    LocalData(DataConnectionsDeclaration),
    LocalMapping(LocalMappingDeclaration),
    PeerRelay(PeerRelayDeclaration),
    RemoteMapping(RemoteMappingsDeclaration),
    User(UserDeclaration),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct DefaultPermissionDeclaration {
    pub allowed_columns: Vec<String>,
    pub allowed_rows: String,
}

pub(crate) fn no_permission_decl() -> Option<Vec<PermissionsDecl>> {
    None
}

pub(crate) fn empty_permission() -> DefaultPermissionDeclaration {
    DefaultPermissionDeclaration {
        allowed_columns: vec![],
        allowed_rows: "false".to_string(),
    }
}

pub(crate) fn no_entitymap() -> Option<EntityMapDecl> {
    None
}

pub(crate) fn no_transformation() -> Transformation {
    Transformation {
        other_to_local_info: "{value}".to_string(),
        local_info_to_other: "{value}".to_string(),
        replace_from: "{value}".to_string(),
    }
}

pub(crate) fn default_false() -> bool {
    false
}
