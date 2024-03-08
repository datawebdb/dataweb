use std::sync::Arc;

use crate::error::Result;
use crate::execute::{request_to_local_queries, request_to_remote_requests};

use crate::model::entity::Information;
use crate::model::query::{
    NewQueryTask, QueryOriginationInfo, QueryRequest, QueryTask, QueryTaskRemote,
    QueryTaskRemoteStatus, QueryTaskStatus, RawQueryRequest,
};
use crate::model::relay::Relay;
use crate::model::user::{NewUser, User, UserAttributes};
use crate::{crud::PgDb, error::MeshError};

use arrow_schema::{Field, SchemaBuilder, SchemaRef};
use datafusion::sql::sqlparser::ast::Statement;
use tracing::debug;
use uuid::Uuid;

use super::planning::EntityContext;
use super::validation::{logical_round_trip, validate_sql};
use super::Requester;

pub async fn validate_sql_and_logical_round_trip(
    sql: &str,
    db: &mut PgDb<'_>,
) -> Result<(String, Statement)> {
    debug!("Parsing SQL to statement: {sql}");
    let (entity_name, statement) = validate_sql(sql)?;
    debug!("pre round trip statement: {statement}");
    let context = create_planning_context(&entity_name, db).await?;
    let statement = logical_round_trip(statement, context)?;
    debug!("post round trip statement: {statement}");
    Ok((entity_name, statement))
}

pub async fn create_planning_context(
    entity_name: &str,
    db: &mut PgDb<'_>,
) -> Result<EntityContext> {
    let entity = db.get_entity(entity_name).await?;
    let information = db.get_information_for_entity(entity.id).await?;
    let schema = information_to_schema(information);
    let context_provider = EntityContext::new(entity_name, schema);
    Ok(context_provider)
}

/// Converts a Vec of [Information] to an arrow [SchemaRef]
pub fn information_to_schema(information: Vec<Information>) -> SchemaRef {
    let mut schema_builder = SchemaBuilder::new();
    for info in information {
        schema_builder.push(Field::new(info.name, info.arrow_dtype.inner, true));
    }
    Arc::new(schema_builder.finish())
}

/// Inspects a [RawQueryRequest] along with the validated x509 certificate fingerprint
/// of the client submitting the request, to determine the [User] which originated the request,
/// and which [Relay] first recieved the request (which could be the local [Relay]).
pub async fn verify_query_origination_information(
    query: &RawQueryRequest,
    db: &mut PgDb<'_>,
    fingerprint: String,
    subject_dn: String,
    issuer_dn: String,
    local_fingerprint: &Arc<String>,
) -> Result<(Requester, User, Relay)> {
    let (direct_requester, requesting_user, originating_relay) = match (
        &query.originating_relay,
        &query.requesting_user,
        &query.request_uuid,
        &query.originating_task_id,
    ) {
        (Some(originator), Some(requesting_user), Some(_), Some(_)) => {
            // If originating relay is set, the request must not come directly from a user. but rather another relay
            // thus the client certificate fingerprint should match a trusted relay, or otherwise we reject the request
            let requesting_relay = Requester::Relay(
                db.get_relay_by_x509_fingerprint(&fingerprint)
                    .await
                    .map_err(|e| {
                        MeshError::DbError(format!(
                            "Rejecting query request from unrecognized \
                 relay with fingerprint {fingerprint} and dn: {subject_dn}, {e}"
                        ))
                    })?,
            );
            (
                requesting_relay,
                requesting_user.clone(),
                originator.clone(),
            )
        }
        (None, None, None, None) => {
            // If originating relay is not set, the request must come directly from a user. The local relay is the originating
            // relay.
            debug!("Checking for user with fingerprint {fingerprint}");
            let user = NewUser {
                x509_sha256: fingerprint.clone(),
                x509_subject: subject_dn.clone(),
                x509_issuer: issuer_dn,
                attributes: UserAttributes::new(),
            };
            let requesting_user = db.upsert_user_by_fingerprint(&user).await?;
            let originator = db
                .get_relay_by_x509_fingerprint(local_fingerprint.as_ref())
                .await?;
            (
                Requester::User(requesting_user.clone()),
                requesting_user,
                originator,
            )
        }
        _ => {
            return Err(MeshError::InvalidQuery(
                "invalid query request: either all of requesting_user, \
        originating_relay, originating_task_id, and request_uuid should be set or none!"
                    .to_string(),
            ))
        }
    };
    Ok((direct_requester, requesting_user, originating_relay))
}

/// Helper function that creates a [QueryRequest], filling in origination information
/// as appropriate depending on the [Requester]
pub async fn create_query_request(
    query: &RawQueryRequest,
    db: &mut PgDb<'_>,
    direct_requester: &Requester,
    requesting_user: &User,
    originating_relay: &Relay,
) -> Result<QueryRequest> {
    let local_req_id = Uuid::new_v4();
    match &direct_requester {
        Requester::Relay(requesting_relay) => {
            let origin_info = QueryOriginationInfo {
                origin_user: Some(requesting_user.clone()),
                origin_relay: Some(originating_relay.clone()),
                origin_task_id: query.originating_task_id,
            };
            let orig_req_id = &query.request_uuid.ok_or(MeshError::InvalidQuery(
                "request_uuid must be set by peer relays when \
                forwarding a request, but found none!"
                    .into(),
            ))?;
            Ok(db
                .create_query_request(
                    &local_req_id,
                    &requesting_relay.id,
                    orig_req_id,
                    &query.sql,
                    &origin_info,
                )
                .await?)
        }
        _ => {
            let origin_info = QueryOriginationInfo {
                origin_user: Some(requesting_user.clone()),
                origin_relay: None,
                origin_task_id: None,
            };
            // We are the origin so we set the origin id to local id
            Ok(db
                .create_query_request(
                    &local_req_id,
                    &originating_relay.id,
                    &local_req_id,
                    &query.sql,
                    &origin_info,
                )
                .await?)
        }
    }
}

/// Helper function that maps a [RawQueryRequest] to [Querys][crate::model::query::Query] for all relevant local
/// data sources and stores the needed info in the database as [QueryTasks][crate::model::query::QueryTask].
pub async fn map_and_create_local_tasks(
    query: &Statement,
    raw_request: &RawQueryRequest,
    entity_name: &str,
    request: &QueryRequest,
    db: &mut PgDb<'_>,
    direct_requester: &Requester,
    requesting_user: &User,
) -> Result<Vec<QueryTask>> {
    let queries = request_to_local_queries(
        db,
        query,
        entity_name,
        raw_request,
        direct_requester,
        requesting_user,
    )
    .await?;
    debug!("Creating {} local tasks!", queries.len());
    let mut tasks = Vec::with_capacity(queries.len());
    for (data_source_id, q) in queries {
        tasks.push(NewQueryTask {
            query_request_id: request.id,
            data_source_id,
            task: q,
            status: QueryTaskStatus::Queued,
        })
    }

    db.create_query_tasks(&tasks).await
}

/// Helper function that maps a [RawQueryRequest] to [Querys][crate::model::query::Query] for all relevant local
/// data sources and stores the needed info in the database as [RemoteQueryTasks][crate::model::query::Query].
pub async fn map_and_create_remote_tasks(
    raw_request: &RawQueryRequest,
    query: &Statement,
    request: &QueryRequest,
    entity_name: &str,
    db: &mut PgDb<'_>,
    requesting_user: User,
    originating_relay: Relay,
) -> Result<Vec<QueryTaskRemote>> {
    let remote_requests = request_to_remote_requests(
        db,
        raw_request,
        query,
        entity_name,
        &request.originator_request_id,
        originating_relay,
        requesting_user,
    )
    .await?;
    let mut remote_tasks = Vec::with_capacity(remote_requests.len());
    for (relay_id, mut remote_request) in remote_requests {
        // Assign a Uuid to this remote request and if no originating task id is set,
        // assign our remote task Uuid as originating_task_id. This signals to all relays
        // in the network to send result streams to us.
        let id = Uuid::new_v4();
        if remote_request.originating_task_id.is_none() {
            remote_request.originating_task_id = Some(id);
        }
        remote_tasks.push(QueryTaskRemote {
            id,
            query_request_id: request.id,
            relay_id,
            task: remote_request,
            status: QueryTaskRemoteStatus::Queued,
        })
    }
    debug!("Creating {} remote tasks!", remote_tasks.len());
    db.create_remote_query_tasks(&remote_tasks).await
}
