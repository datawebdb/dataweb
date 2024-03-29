use std::collections::HashMap;

use crate::schema::users;

use diesel::prelude::*;
use diesel::{prelude::Insertable, AsExpression, FromSqlRow};
use diesel_as_jsonb::AsJsonb;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A user is any actor which requests data from the [Relay][crate::model::relay::Relay] network.
/// It could be an individual person entity or it could be a non-person entity,
/// such as an application which wishes to consume arrow data from the network. All users must
/// present an x509 client certificate signed by a CA trusted by the local relay, but they do not
/// need to be registered in advance unless they require elevated permissions beyond the defaults
/// granted.
#[derive(Clone, Serialize, Deserialize, Queryable, Selectable, Identifiable, Debug, PartialEq)]
pub struct User {
    pub id: Uuid,
    /// Sha256 Fingerprint of the DER encoded certificate
    pub x509_sha256: String,
    /// X509 Subject Distinguished Name
    pub x509_subject: String,
    /// X509 Issuer Distinguished NAme
    pub x509_issuer: String,
    /// Abritrary user attributes
    pub attributes: UserAttributes,
}

/// Stores arbitrary user attributes which can be used for access control decisions.
#[derive(Debug, PartialEq, Serialize, Deserialize, AsJsonb, Clone)]
pub struct UserAttributes {
    /// Controls whether this user should be permitted access to configure the [Relay][crate::model::relay::Relay]
    /// via the /admin endpoints.
    #[serde(default = "default_admin")]
    pub is_admin: bool,
    /// Arbitrary, user defined attributes.
    #[serde(default = "default_attributes")]
    pub misc: HashMap<String, String>,
}

fn default_admin() -> bool {
    false
}

fn default_attributes() -> HashMap<String, String> {
    HashMap::new()
}

impl UserAttributes {
    pub fn new() -> Self {
        Self {
            is_admin: false,
            misc: HashMap::new(),
        }
    }

    pub fn with_is_admin(mut self, is_admin: bool) -> Self {
        self.is_admin = is_admin;
        self
    }

    pub fn with_attributes(mut self, attributes: HashMap<String, String>) -> Self {
        self.misc = attributes;
        self
    }
}

impl Default for UserAttributes {
    fn default() -> Self {
        Self::new()
    }
}

/// Used to create a new [User] object in the database.
#[derive(
    Clone, Serialize, Deserialize, Insertable, Queryable, Selectable, Debug, PartialEq, AsChangeset,
)]
#[diesel(table_name = users)]
pub struct NewUser {
    /// Sha256 Fingerprint of the DER encoded certificate
    pub x509_sha256: String,
    /// X509 Subject Distinguished Name
    pub x509_subject: String,
    /// X509 Issuer Distinguished NAme
    pub x509_issuer: String,
    /// Abritrary user attributes
    pub attributes: UserAttributes,
}
