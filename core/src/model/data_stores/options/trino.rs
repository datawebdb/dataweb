use serde::{Deserialize, Serialize};

/// Holds settings needed to connect to a Trino cluster
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrinoConnection {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: String,
    pub secure: bool,
}

/// Holds settings needed to query a specific table via a trino cluster
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrinoSource {}
