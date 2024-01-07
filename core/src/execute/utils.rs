use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::execute::{request_to_local_queries, request_to_remote_requests};

use crate::model::mappings::{RemoteEntityMapping, Transformation};
use crate::model::query::{
    NewQueryTask, OriginatorEntityMapping, OriginatorInfoMapping, OriginatorMappings,
    QueryOriginationInfo, QueryRequest, QueryTask, QueryTaskRemote, QueryTaskRemoteStatus,
    QueryTaskStatus, RawQueryRequest, ScopedOriginatorMappings, SubstitutionBlocks,
};
use crate::model::relay::Relay;
use crate::model::user::{NewUser, User};
use crate::{crud::PgDb, error::MeshError};

use tracing::debug;
use uuid::Uuid;

use super::Requester;

/// Helper function which substitutes in a subquery template in place of a
/// [SourceSubstitution][crate::model::query::SourceSubstitution]
/// which maps a remote [Entity][crate::model::entity::Entity] to a local one. Local
/// [Information][crate::model::entity::Information] may need to be derived from multiple
/// remote informations, or even via aggregations or joins involving multiple remote informations.
/// This is analgous to substituting a subquery into a fully formed SQL statement in place of a
/// table identifier. Since this function deals with substituting a template into another template,
/// it must deal with the potential of conflicting substitution keys or number of capture braces.
pub fn compose_derived_source_substitution(
    mut original_sql: String,
    in_blocks: &SubstitutionBlocks,
    left_capture: &String,
    right_capture: &String,
    sub_into_pattern: String,
    entity_map: &RemoteEntityMapping,
    out_blocks: &mut SubstitutionBlocks,
) -> (String, Option<ScopedOriginatorMappings>) {
    let mut sub_sql = entity_map.sql.clone();
    let sub_blocks = entity_map.substitution_blocks.clone();
    let sub_left_capture = "{".repeat(sub_blocks.num_capture_braces);
    let sub_right_capture = "}".repeat(sub_blocks.num_capture_braces);
    let different_num_capture_braces =
        in_blocks.num_capture_braces != sub_blocks.num_capture_braces;

    let new_scope = Uuid::new_v4().to_string();
    let mut remote_as_originator_mappings = OriginatorEntityMapping {
        originator_entity_name: entity_map.remote_entity_name.clone(),
        originator_info_map: HashMap::new(),
    };
    let mut new_info_subs = HashMap::with_capacity(sub_blocks.info_substitutions.len());
    for (key, mut sub) in sub_blocks.info_substitutions.into_iter() {
        sub.scope = new_scope.clone();
        remote_as_originator_mappings.originator_info_map.insert(
            sub.info_name.clone(),
            OriginatorInfoMapping {
                originator_info_name: sub.info_name.clone(),
                transformation: Transformation {
                    other_to_local_info: "{v}".to_string(),
                    local_info_to_other: "{v}".to_string(),
                    replace_from: "{v}".to_string(),
                },
            },
        );
        if different_num_capture_braces
            || in_blocks.info_substitutions.contains_key(&key)
            || in_blocks.source_substitutions.contains_key(&key)
        {
            let new_key = Uuid::new_v4();
            let new_pattern = format!("{left_capture}{new_key}{right_capture}");
            let old_pattern = format!("{sub_left_capture}{key}{sub_right_capture}");
            sub_sql = sub_sql.replace(&old_pattern, &new_pattern);

            new_info_subs.insert(new_key.to_string(), sub);
        } else {
            new_info_subs.insert(key, sub);
        }
    }

    let mut new_source_subs = HashMap::with_capacity(sub_blocks.source_substitutions.len());
    for (key, sub) in sub_blocks.source_substitutions.into_iter() {
        if different_num_capture_braces {
            let new_key = Uuid::new_v4();
            let new_pattern = format!("{left_capture}{new_key}{right_capture}");
            let old_pattern = format!("{sub_left_capture}{key}{sub_right_capture}");
            sub_sql = sub_sql.replace(&old_pattern, &new_pattern);
            new_source_subs.insert(new_key.to_string(), sub);
        } else {
            new_source_subs.insert(key, sub);
        }
    }

    let replacement = format!("({})", sub_sql);
    original_sql = original_sql.replace(sub_into_pattern.as_str(), replacement.as_str());

    out_blocks.info_substitutions.extend(new_info_subs);
    out_blocks.source_substitutions.extend(new_source_subs);

    let scoped_mappings = if remote_as_originator_mappings.originator_info_map.is_empty() {
        None
    } else {
        let originator_mappings = OriginatorMappings {
            inner: HashMap::from_iter(vec![(
                entity_map.remote_entity_name.clone(),
                remote_as_originator_mappings,
            )]),
        };
        Some(ScopedOriginatorMappings {
            inner: HashMap::from_iter(vec![(new_scope, originator_mappings)]),
        })
    };
    (original_sql, scoped_mappings)
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
                    &query.substitution_blocks,
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
                    &query.substitution_blocks,
                    &origin_info,
                )
                .await?)
        }
    }
}

/// Helper function that maps a [RawQueryRequest] to [Querys][crate::model::query::Query] for all relevant local
/// data sources and stores the needed info in the database as [QueryTasks][crate::model::query::QueryTask].
pub async fn map_and_create_local_tasks(
    query: &RawQueryRequest,
    request: &QueryRequest,
    db: &mut PgDb<'_>,
    direct_requester: &Requester,
    requesting_user: &User,
) -> Result<Vec<QueryTask>> {
    let queries = request_to_local_queries(db, query, direct_requester, requesting_user).await?;
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
    query: &RawQueryRequest,
    request: &QueryRequest,
    db: &mut PgDb<'_>,
    requesting_user: User,
    originating_relay: Relay,
) -> Result<Vec<QueryTaskRemote>> {
    let remote_requests = request_to_remote_requests(
        db,
        query,
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
