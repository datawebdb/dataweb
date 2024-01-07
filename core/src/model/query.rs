use super::{data_stores::DataSource, mappings::Transformation, relay::Relay, user::User};
use crate::schema::{incoming_flight_streams, query_request, query_task, query_task_remote};

use arrow_schema::Schema;
use diesel::prelude::*;
use diesel::{AsExpression, FromSqlRow};
use diesel_as_jsonb::AsJsonb;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
    /// A Sql template string, e.g. "select {info} from {source}"
    pub sql: String,
    pub substitution_blocks: SubstitutionBlocks,
    /// This is the globally unique [Uuid] for the query request, which is required for handling
    /// cyclical relay network topologies. If the same Uuid is encountered twice, the request should be
    /// acknowledged as already in progress.
    pub request_uuid: Option<Uuid>,
    /// This is the [User] that submitted the original request to the originating_relay
    pub requesting_user: Option<User>,
    /// This is the original requesting [Relay], which may not be directly connected to the local relay.
    #[serde(default = "no_relay")]
    pub originating_relay: Option<Relay>,
    /// This Uuid identifies the remote task on the originating relay which ultimately triggered this request
    pub originating_task_id: Option<Uuid>,
    /// In the case of multiple hops between the originating_relay and the ultimate executing [Relay],
    /// we need to keep track of how Entity and Information names map back to the originator,
    /// since the executor will short-circuit the intermediate hops and send data over Flight gRPC
    /// directly to the originator, and each relay only permanently stores mappings to its direct neighbors.
    #[serde(default = "no_mappings")]
    pub originator_mappings: Option<ScopedOriginatorMappings>,
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

fn no_mappings() -> Option<ScopedOriginatorMappings> {
    None
}

fn no_uuid() -> Option<Uuid> {
    None
}

/// A component of a [OriginatorEntityMapping] for a specific Entity name-space
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct OriginatorInfoMapping {
    pub originator_info_name: String,
    pub transformation: Transformation,
}

/// A component of a [OriginatorMappings]
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct OriginatorEntityMapping {
    pub originator_entity_name: String,
    pub originator_info_map: HashMap<String, OriginatorInfoMapping>,
}

/// A scoped version of [OriginatorMappings]. As a query propagates the network,
/// any [Relay] may introduce a [RemoteEntityMapping][crate::model::mappings::RemoteEntityMapping].
/// Each new subquery template added requires its own [OriginatorMappings] which is specific
/// to the [InfoSubstitution]s of that subquery. This effectively allows for different "originator"
/// [Relay]s for each [InfoSubstitution].
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct ScopedOriginatorMappings {
    pub inner: HashMap<String, OriginatorMappings>,
}
/// Keeps track of how to transform local Information back to Information on
/// the originating [Relay], which may be an arbitrary number of hops away. The
/// local [Relay] only stores the mappings to its direct neighbors, but must be able
/// to map its Information to the Information of any other [Relay] in the network.
/// This is possible by incrementally composing [Transformation]s as a query propagates
/// through the network.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct OriginatorMappings {
    /// Left = Local Relay name, Right = Originating Relay name.
    /// Outer HashMap maps entity name to entity name + inner map
    /// which maps info name to info name (scoped to a given local entity name)
    pub inner: HashMap<String, OriginatorEntityMapping>,
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
    pub substitution_blocks: SubstitutionBlocks,
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
/// number of additional Relays many hops away from the local relay. Each remote task
/// will execute a do_put call to the local relay containing the QueryTaskRemote id.
/// The local relay will tie the QueryTaskRemote to an arbitrary number of [FlightStream]s,
/// one for each do_put call it received.
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

/// Contains a explicit description of how a SQL template should be transformed
/// into a SQL statement that can be executed against local [DataSource]s. Portions
/// of the SQL template will be wrapped in curly braces, and either interpreted as
/// Information or a description of which [DataSource]s to include
#[derive(Debug, PartialEq, Serialize, Deserialize, AsJsonb, Clone)]
pub struct SubstitutionBlocks {
    pub info_substitutions: HashMap<String, InfoSubstitution>,
    pub source_substitutions: HashMap<String, SourceSubstitution>,
    #[serde(default = "default_capture_braces")]
    pub num_capture_braces: usize,
}

fn default_capture_braces() -> usize {
    1
}

/// A portion of a SQL template expressed in terms of local Information that can be substituted
/// into DataField paths. E.g. select {user_name} from {table} may be converted into
/// select user.full_name from {table} for a table containing a field path user.full_name mapped to
/// the specified local Information.
#[derive(Debug, PartialEq, Serialize, Deserialize, AsJsonb, Clone)]
pub struct InfoSubstitution {
    pub entity_name: String,
    pub info_name: String,
    /// Defines a frame of refrence for Information transformations.
    /// See [ScopedOriginatorMappings] for detailed discussion.
    #[serde(default = "default_scope")]
    pub scope: String,
    /// Whether to include the mapped Information name in the returned Arrow data.
    /// If true, the SQL statement will include "transform({data_field_path}) as {entity_name}_{info_name}"
    #[serde(default = "default_true")]
    pub include_info: bool,
    /// If true, excludes the info alias when include_info is also true, so that the SQL statement will instead
    /// include just "transform({data_field_path})"
    #[serde(default = "default_false")]
    pub exclude_info_alias: bool,
    /// Whether to include the data field in the returned Arrow data. Cannot be false if include_info is
    /// also false.
    #[serde(default = "default_false")]
    pub include_data_field: bool,
}

/// The default scope identifier for [InfoSubstitution]s passed directly by a [User] to the origin [Relay].
pub(crate) fn default_scope() -> String {
    "origin".to_string()
}

fn default_true() -> bool {
    true
}

fn default_false() -> bool {
    false
}

/// Used to substitute in table identifiers into a SQL template. Each [Relay] also applies
/// relevant access controls
#[derive(Debug, PartialEq, Serialize, Deserialize, AsJsonb, Clone)]
pub enum SourceSubstitution {
    /// Infer all available sources which contain Information on any of the referenced Entities
    AllSourcesWith(Vec<String>),
    /// Explicitly passed list of [DataSource]s to query, by uuid
    SourceList(Vec<Uuid>),
}
