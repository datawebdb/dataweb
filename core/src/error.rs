use std::{error::Error, fmt, result};

use datafusion::arrow;

use serde_json::Value;
use tokio::sync::mpsc::error::TryRecvError;

use crate::model::query::QueryRequest;

pub type Result<T, E = MeshError> = result::Result<T, E>;

#[derive(Debug)]
pub enum MeshError {
    Internal(String),
    DbError(String),
    Messaging(String),
    BadMessage((u64, String)),
    EmptyRecv(String),
    SerDe(String),
    NotImplemented(String),
    InvalidQuery(String),
    InvalidTransform(Value),
    RemoteError(String),
    DuplicateQueryRequest(Box<QueryRequest>),
    EmptyQuery,
}

impl Error for MeshError {}

impl fmt::Display for MeshError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MeshError::Internal(s) => write!(f, "Unexpected internal error: {}", s),
            MeshError::DbError(s) => write!(f, "Database related error: {}", s),
            MeshError::Messaging(s) => write!(f, "Message broker related error: {}", s),
            MeshError::BadMessage((id, s)) => {
                write!(f, "Invalid message with id {} and error {}", id, s)
            }
            MeshError::SerDe(s) => write!(f, "SerDe related error: {}", s),
            MeshError::EmptyRecv(s) => write!(f, "No message recieved: {}", s),
            MeshError::NotImplemented(s) => write!(f, "Unimplemeneted feature error: {}", s),
            MeshError::InvalidTransform(_) => write!(f, "Invalid Transformation"),
            MeshError::InvalidQuery(s) => write!(f, "invalid query: {}", s),
            MeshError::RemoteError(s) => write!(f, "Issue related to a remote relay: {}", s),
            MeshError::DuplicateQueryRequest(q) => write!(
                f,
                "Query {} has already been processed!",
                q.originator_request_id
            ),
            MeshError::EmptyQuery => write!(f, "no matching values were found for query"),
        }
    }
}

impl From<diesel::result::Error> for MeshError {
    fn from(e: diesel::result::Error) -> Self {
        MeshError::DbError(e.to_string())
    }
}

impl From<diesel::result::ConnectionError> for MeshError {
    fn from(e: diesel::result::ConnectionError) -> Self {
        MeshError::DbError(e.to_string())
    }
}

impl From<object_store::Error> for MeshError {
    fn from(e: object_store::Error) -> Self {
        MeshError::Internal(e.to_string())
    }
}

impl From<datafusion::common::DataFusionError> for MeshError {
    fn from(e: datafusion::common::DataFusionError) -> Self {
        MeshError::Internal(e.to_string())
    }
}

impl From<arrow::error::ArrowError> for MeshError {
    fn from(e: arrow::error::ArrowError) -> Self {
        MeshError::Internal(e.to_string())
    }
}

impl From<url::ParseError> for MeshError {
    fn from(e: url::ParseError) -> Self {
        MeshError::Internal(e.to_string())
    }
}

impl From<regex::Error> for MeshError {
    fn from(e: regex::Error) -> Self {
        MeshError::Internal(e.to_string())
    }
}

#[cfg(feature = "rabbitmq")]
impl From<amqprs::error::Error> for MeshError {
    fn from(e: amqprs::error::Error) -> Self {
        MeshError::Messaging(e.to_string())
    }
}

impl From<serde_json::Error> for MeshError {
    fn from(e: serde_json::Error) -> Self {
        MeshError::SerDe(e.to_string())
    }
}

impl From<TryRecvError> for MeshError {
    fn from(e: TryRecvError) -> Self {
        match e {
            TryRecvError::Disconnected => MeshError::Messaging(e.to_string()),
            TryRecvError::Empty => MeshError::EmptyRecv(e.to_string()),
        }
    }
}

impl From<object_store::path::Error> for MeshError {
    fn from(e: object_store::path::Error) -> Self {
        MeshError::Internal(e.to_string())
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for MeshError {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        MeshError::SerDe(e.to_string())
    }
}

impl From<std::io::Error> for MeshError {
    fn from(e: std::io::Error) -> Self {
        MeshError::SerDe(e.to_string())
    }
}

impl From<http::uri::InvalidUri> for MeshError {
    fn from(e: http::uri::InvalidUri) -> Self {
        MeshError::RemoteError(e.to_string())
    }
}

impl From<tonic::transport::Error> for MeshError {
    fn from(e: tonic::transport::Error) -> Self {
        MeshError::RemoteError(e.to_string())
    }
}

impl From<datafusion::sql::sqlparser::parser::ParserError> for MeshError {
    fn from(e: datafusion::sql::sqlparser::parser::ParserError) -> Self {
        MeshError::InvalidQuery(e.to_string())
    }
}
