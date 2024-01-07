use crate::schema::users;

use diesel::prelude::*;
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
}