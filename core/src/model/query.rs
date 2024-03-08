use super::{data_stores::DataSource, relay::Relay, user::User};
use crate::schema::{incoming_flight_streams, query_request, query_task, query_task_remote};

use arrow_schema::Schema;
use diesel::prelude::*;
use diesel::{AsExpression, FromSqlRow};
use diesel_as_jsonb::AsJsonb;
use serde::{Deserialize, Serialize};

use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, AsJsonb, PartialEq)]
/// Contains all of the information that controls how a
/// [QueryRunner][crate::execute::data_stores::QueryRunner]
/// will query the underlying data. At this stage, the SQL is fully
/// resolved into something that can be executed on a query engine
/// directly.
pub struct Query {
    pub sql: String,
    /// The [QueryRunner][crate::execute::data_stores::QueryRunner] should
    /// return data with this schema if specified. Otherwise, the schema is
    /// inferred by the data returned by the [DataSource].
    pub return_schema: Option<Schema>,
}

#[derive(Serialize, Deserialize, Debug, AsJsonb, PartialEq)]
/// This is the original, unresolved query request which is either
/// recieved directly by a [User] or indirectly via a peered [Relay].
/// Each relay processing a QueryRequest will need to resolve it
/// to [Query] objects which can be executed against local [DataSource]s.
pub struct RawQueryRequest {
    /// A raw SQL string, expressed in terms of [Entity][crate::model::entity::Entity]
    /// and [Information][crate::model::entity::Information].
    pub sql: String,
    /// This is the globally unique [Uuid] for the query request, which is required for handling
    /// cyclical relay network topologies. If the same Uuid is encountered twice, the request should be
    /// acknowledged as already in progress. This corresponds to the Uuid of the [QueryRequest] on the
    /// originating [Relay].
    pub request_uuid: Option<Uuid>,
    /// This is the [User] that submitted the original request to the originating_relay
    pub requesting_user: Option<User>,
    /// This is the original requesting [Relay], which may not be directly connected to the local relay.
    #[serde(default = "no_relay")]
    pub originating_relay: Option<Relay>,
    /// This Uuid identifies the remote task on the originating relay which ultimately triggered this request
    /// This corresponds to the Uuid of the [QueryTaskRemote] on the originating relay.
    pub originating_task_id: Option<Uuid>,
    /// If Passed, each relay will cast the returned RecordBatchStream to the requested [Schema].
    /// If not passed, the schema may vary slightly based on the QueryRunner and DataSource, due to
    /// e.g. how the schema of a JSON or CSV file is inferred.
    #[serde(default = "no_schema")]
    pub return_arrow_schema: Option<Schema>,
}

fn no_schema() -> Option<Schema> {
    None
}

fn no_user() -> Option<User> {
    None
}

fn no_relay() -> Option<Relay> {
    None
}

fn no_uuid() -> Option<Uuid> {
    None
}

#[derive(
    Serialize, Deserialize, Queryable, Selectable, Identifiable, Associations, Debug, PartialEq,
)]
#[diesel(belongs_to(Relay))]
#[diesel(table_name = query_request)]
/// Fully formed [RawQueryRequest] with metadata added
/// about where the request was recieved from and its current
/// status.
pub struct QueryRequest {
    pub id: Uuid,
    /// This is the uuid of the [QueryRequest] for the [Relay] which directly recieved the request
    /// from a [User]. In case of circular loops in the Relay network, each relay must only process
    /// each originator_request_id exactly once.
    pub originator_request_id: Uuid,
    pub sql: String,
    pub relay_id: Uuid,
    pub origin_info: QueryOriginationInfo,
}

/// Contains information about the origin of a [QueryRequest], which
/// indicates the [Relay] which recieved the original query request and the
/// the id of the corresponding [QueryTaskRemote] as well as the [User]
/// which initiated the request.
#[derive(Debug, PartialEq, Serialize, Deserialize, AsJsonb)]
pub struct QueryOriginationInfo {
    #[serde(default = "no_user")]
    pub origin_user: Option<User>,
    /// If None that means the parent [QueryRequest] is the original one.
    #[serde(default = "no_relay")]
    pub origin_relay: Option<Relay>,
    /// If None that means the parent [QueryRequest] is the original one.
    #[serde(default = "no_uuid")]
    pub origin_task_id: Option<Uuid>,
}

#[derive(Queryable, Selectable, Identifiable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(QueryRequest))]
#[diesel(belongs_to(DataSource))]
#[diesel(table_name = query_task)]
/// An individual task required to complete a [QueryRequest] which
/// will be run on a [DataSource] of the local relay.
pub struct QueryTask {
    pub id: Uuid,
    pub query_request_id: Uuid,
    pub data_source_id: Uuid,
    pub task: Query,
    pub status: QueryTaskStatus,
}

/// Used to create a new [QueryTask] object in the database
#[derive(Queryable, Selectable, Insertable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(QueryRequest))]
#[diesel(table_name = query_task)]
pub struct NewQueryTask {
    pub query_request_id: Uuid,
    pub data_source_id: Uuid,
    pub task: Query,
    pub status: QueryTaskStatus,
}

/// Represents the status of a [QueryTask]. Only used in asynchronous execution mode.
#[derive(Serialize, Deserialize, Debug, PartialEq, diesel_derive_enum::DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::QueryTaskStatus"]
pub enum QueryTaskStatus {
    Queued,
    InProgress,
    Complete,
    Failed,
}

#[derive(Insertable, Queryable, Selectable, Identifiable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(QueryRequest))]
#[diesel(table_name = query_task_remote)]
/// A QueryTaskRemote is created when the local [Relay] propagates a [RawQueryRequest] to
/// a peered [Relay]. The peered Relay may in turn propagate the request to an aribtrary
/// number of additional Relays many hops away from the local relay.
///
/// In async execution mode, each remote task will execute a do_put call to the local relay
/// containing the QueryTaskRemote id. The local relay will tie the QueryTaskRemote to
/// an arbitrary number of [FlightStream]s, one for each do_put call it received.
pub struct QueryTaskRemote {
    pub id: Uuid,
    pub query_request_id: Uuid,
    pub relay_id: Uuid,
    pub task: RawQueryRequest,
    pub status: QueryTaskRemoteStatus,
}

/// Represents the status of a [QueryTaskRemote]
#[derive(Serialize, Deserialize, Debug, PartialEq, diesel_derive_enum::DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::QueryTaskRemoteStatus"]
pub enum QueryTaskRemoteStatus {
    Queued,
    Submitted,
    Complete,
    Failed,
}

/// Used to create a new [QueryTaskRemote] object in the database.
#[derive(Queryable, Insertable, Selectable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(QueryRequest))]
#[diesel(table_name = query_task_remote)]
/// Insertable form of [QueryTaskRemote]
pub struct NewQueryTaskRemote {
    pub query_request_id: Uuid,
    pub relay_id: Uuid,
    pub task: RawQueryRequest,
    pub status: QueryTaskRemoteStatus,
}

#[derive(Queryable, Selectable, Identifiable, Associations, Debug, PartialEq, AsChangeset)]
#[diesel(belongs_to(QueryTaskRemote))]
#[diesel(table_name = incoming_flight_streams)]
/// [QueryTaskRemote]s flow through the network of [Relay]s and ultimately result
/// in an unknown, arbitrary number of [Query]s running throughout the mesh.
/// Each [Query] executed remotely results in a FlightStream sent back to the local
/// node which originated the first [QueryRequest].
pub struct FlightStream {
    pub id: Uuid,
    /// The id of the local [QueryTaskRemote] object
    pub query_task_remote_id: Uuid,
    /// Sha256 of the remote [Relay]s der encoded x509 certificate
    pub remote_fingerprint: String,
    pub flight_id: Uuid,
    pub status: FlightStreamStatus,
}

/// Used to create a [FlightStream] object in the database
#[derive(Queryable, Insertable, Selectable, Associations, Debug, PartialEq, AsChangeset)]
#[diesel(belongs_to(QueryTaskRemote))]
#[diesel(table_name = incoming_flight_streams)]
/// Insertable form of [FlightStream]
pub struct NewFlightStream {
    /// The id of the local [QueryTaskRemote] object
    pub query_task_remote_id: Uuid,
    /// Sha256 of the remote [Relay]s der encoded x509 certificate
    pub remote_fingerprint: String,
    pub flight_id: Uuid,
    pub status: FlightStreamStatus,
}

/// Indicates the status of a [FlightStream]
#[derive(Serialize, Deserialize, Debug, PartialEq, diesel_derive_enum::DbEnum)]
#[ExistingTypePath = "crate::schema::sql_types::FlightStreamStatus"]
pub enum FlightStreamStatus {
    /// A do_put call was received, but it was rejected as not valid
    Invalid,
    /// A valid do_put call was received and is currently being processed
    Started,
    /// A valid do_put call was received but failed during execution
    Failed,
    /// A valid do_put call was received and completed successfully
    Complete,
}
