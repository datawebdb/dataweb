use serde::{Deserialize, Serialize};

use super::{no_permission_decl, user::PermissionsDecl};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PeerRelayDeclaration {
    pub name: String,
    pub rest_endpoint: String,
    pub flight_endpoint: String,
    pub x509_cert: String,
    #[serde(default = "no_permission_decl")]
    pub permissions: Option<Vec<PermissionsDecl>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ResolvedPeerRelayDeclaration {
    pub name: String,
    pub rest_endpoint: String,
    pub flight_endpoint: String,
    pub x509_cert: Vec<u8>,
    #[serde(default = "no_permission_decl")]
    pub permissions: Option<Vec<PermissionsDecl>>,
}
