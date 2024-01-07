use crate::schema::relays;

use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Contains the needed information to identify an individual Relay in the network.
/// All REST and flight gRPC connections are secured via mTLS, and the client Relay
/// can be identified based on the sha256 hash of its x509 client certificate.
#[derive(
    Serialize,
    Deserialize,
    Queryable,
    Insertable,
    Selectable,
    Identifiable,
    Debug,
    PartialEq,
    Hash,
    Eq,
    Clone,
)]
pub struct Relay {
    pub id: Uuid,
    pub name: String,
    /// The URL endpoint where this relay can be reached.
    pub rest_endpoint: String,
    /// The URL endpoint for the remote flight service.
    pub flight_endpoint: String,
    /// Sha256 Fingerprint of the DER encoded certificate
    pub x509_sha256: String,
    /// X509 Subject Distinguished Name
    pub x509_subject: String,
    /// X509 Issuer Distinguished NAme
    pub x509_issuer: String,
}

/// Used to create a new [Relay] object in the database
#[derive(Queryable, Selectable, Insertable, Debug, PartialEq, AsChangeset)]
#[diesel(table_name = relays)]
pub struct NewRelay {
    pub name: String,
    /// The URL endpoint where this relay can be reached.
    pub rest_endpoint: String,
    /// The URL endpoint for the remote flight service.
    pub flight_endpoint: String,
    /// Sha256 Fingerprint of the DER encoded certificate
    pub x509_sha256: String,
    /// X509 Subject Distinguished Name
    pub x509_subject: String,
    /// X509 Issuer Distinguished NAme
    pub x509_issuer: String,
}
