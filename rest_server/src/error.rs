use std::{error::Error, fmt};

use actix_web::error;
use tracing::log::error;

pub(crate) type Result<T, E = RelayError> = std::result::Result<T, E>;

#[derive(Debug)]
pub(crate) struct RelayError {
    pub(crate) msg: String,
}

impl Error for RelayError {}

impl fmt::Display for RelayError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Relay server error: {}", self.msg)
    }
}

impl error::ResponseError for RelayError {}

impl From<mesh::error::MeshError> for RelayError {
    fn from(e: mesh::error::MeshError) -> Self {
        error!("Relay returning MeshError to caller: {e}");
        RelayError { msg: e.to_string() }
    }
}
