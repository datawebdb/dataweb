pub mod options;

use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{data_connection, data_field, data_source};

use self::options::{ConnectionOptions, SourceOptions};

/// A DataConnection is a collection of [DataSource]s which can be queried via a common
/// connection. This could be an invidual database or an ObjectStore.
#[derive(
    Serialize, Deserialize, Queryable, Identifiable, Selectable, Debug, PartialEq, Eq, Hash,
)]
#[diesel(table_name = data_connection)]
pub struct DataConnection {
    pub id: Uuid,
    pub name: String,
    pub connection_options: ConnectionOptions,
}

/// An individual table in a database or any collection of physical data which can be queried.
/// It contains [DataField]s.
#[derive(
    Serialize,
    Deserialize,
    Queryable,
    Selectable,
    Identifiable,
    Associations,
    Debug,
    PartialEq,
    Eq,
    Hash,
)]
#[diesel(belongs_to(DataConnection))]
#[diesel(table_name = data_source)]
pub struct DataSource {
    pub id: Uuid,
    pub name: String,
    pub source_sql: String,
    pub data_connection_id: Uuid,
    pub source_options: SourceOptions,
}

/// An invidual column or unit of [Information][crate::model::entity::Information]
/// found in a [DataSource]. Identified in SQL queries by the path parameter.
/// E.g. select DataField.path from DataSource.name -> gives this DataField
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Identifiable, Associations, Debug, PartialEq,
)]
#[diesel(belongs_to(DataSource))]
#[diesel(table_name = data_field)]
pub struct DataField {
    pub id: Uuid,
    pub name: String,
    pub data_source_id: Uuid,
    /// Represents the possibly nested path to the field within an individual record
    /// of the parent [DataSource], JSON path syntax if nested e.g. '$.nested.array.\[1\].field'
    pub path: String,
}

/// Used to create a new [DataSource] object in the database
#[derive(Queryable, Selectable, Insertable, Associations, Debug, PartialEq, AsChangeset)]
#[diesel(belongs_to(DataConnection))]
#[diesel(table_name = data_source)]
pub struct NewDataSource {
    pub name: String,
    pub source_sql: String,
    pub data_connection_id: Uuid,
    pub source_options: SourceOptions,
}

/// Used to create a new [DataField] object in the database
#[derive(Queryable, Selectable, Insertable, Associations, Debug, PartialEq, AsChangeset)]
#[diesel(belongs_to(DataSource))]
#[diesel(table_name = data_field)]
pub struct NewDataField {
    pub name: String,
    pub data_source_id: Uuid,
    pub path: String,
}
