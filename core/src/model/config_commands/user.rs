use crate::model::user::UserAttributes;
use serde::{Deserialize, Serialize};

use super::no_permission_decl;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ResolvedUserDeclaration {
    pub x509_cert: Vec<u8>,
    pub attributes: UserAttributes,
    #[serde(default = "no_permission_decl")]
    pub permissions: Option<Vec<PermissionsDecl>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct UserDeclaration {
    pub x509_cert_file: String,
    #[serde(default)]
    pub attributes: UserAttributes,
    #[serde(default = "no_permission_decl")]
    pub permissions: Option<Vec<PermissionsDecl>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct SourcePermissionDecl {
    pub data_source_name: String,
    pub allowed_columns: Vec<String>,
    pub allowed_rows: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct PermissionsDecl {
    pub data_con_name: String,
    pub source_permissions: Vec<SourcePermissionDecl>,
}
