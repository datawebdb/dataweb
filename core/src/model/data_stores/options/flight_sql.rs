use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlightSQLSource {}

/// Information needed to identify and connect to a FlightSQL Endpoint
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FlightSqlConnection {
    pub endpoint: String,
    pub auth: FlightSQLAuth,
    pub supports_substrait: bool,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlightSQLAuth {
    Basic(BasicFlightSQLAuth),
    PKI(PKIFlightSQLAuth),
    /// While not recommended, this option enables connecting to a FlightSQL endpoint
    /// which requires no authentication.
    Unsecured,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BasicFlightSQLAuth {
    /// The plaintext username for basic auth
    pub username: String,
    /// An environment variable which will hold the password for basic auth.
    /// Note that this is NOT the plaintext password literally.
    pub password: String,
}

#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PKIFlightSQLAuth {
    /// The public x509 cert pem file used to auth with the FlightSQL server
    pub client_cert_file: String,
    /// The private key pem file used to validate the public client cert
    pub client_key_file: String,
    /// The bundle of trusted CAcerts for validating the FlightSQL server
    pub ca_cert_bundle: String,
}
