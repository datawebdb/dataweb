pub mod data_stores;
mod map_local;
mod map_remote;
pub(crate) mod parse_utils;
pub(crate) mod planning;
pub mod result_manager;
pub mod utils;
pub mod validation;

use std::collections::HashMap;
use std::ops::ControlFlow;

use crate::error::Result;
use crate::model::access_control::SourcePermission;
use crate::model::data_stores::DataSource;

use crate::model::query::RawQueryRequest;
use crate::model::relay::Relay;
use crate::model::user::User;
use crate::{crud::PgDb, error::MeshError, model::query::Query};

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::ast::{TableFactor, VisitMut, VisitorMut};
use tracing::debug;
use uuid::Uuid;

use self::map_local::map_sql;
use self::map_remote::map_remote_request;

struct TableVisitor<F>(F);

impl<E, F: FnMut(&mut TableFactor) -> ControlFlow<E>> VisitorMut for TableVisitor<F> {
    type Break = E;

    fn post_visit_table_factor(
        &mut self,
        table_factor: &mut TableFactor,
    ) -> ControlFlow<Self::Break> {
        self.0(table_factor)
    }
}

pub fn visit_table_factor_mut<V, E, F>(v: &mut V, f: F) -> ControlFlow<E>
where
    V: VisitMut,
    F: FnMut(&mut TableFactor) -> ControlFlow<E>,
{
    let mut visitor = TableVisitor(f);
    v.visit(&mut visitor)?;
    ControlFlow::Continue(())
}

/// Represents either the [Relay] or [User] from which a request was directly
/// received. This is may be distinct from the originating_relay which originated
/// the request, but may not be the relay from which the request was directly
/// received.
#[derive(Debug)]
pub enum Requester {
    User(User),
    Relay(Relay),
}

/// Resolves a [RawQueryRequest] to the corresponding [Query]s which must be
/// executed on the local relay to complete the request.
pub async fn request_to_local_queries(
    db: &mut PgDb<'_>,
    query: &Statement,
    entity_name: &str,
    raw_request: &RawQueryRequest,
    direct_requester: &Requester,
    requesting_user: &User,
) -> Result<Vec<(Uuid, Query)>> {
    let sources = db.get_mappings_by_entity_names(vec![entity_name]).await?;

    let mut queries = Vec::with_capacity(sources.len());
    for ((_, source), mappings) in sources {
        let mut info_map_lookup = HashMap::with_capacity(mappings.len());
        for (_, info, field, map) in mappings.iter() {
            if info_map_lookup
                .insert(info.name.as_str(), (field, map))
                .is_some()
            {
                return Err(MeshError::InvalidQuery(format!(
                    "Found duplicate mapping for {} and source {}",
                    info.name, source.id
                )));
            }
        }
        let permission =
            evaluate_permission_policies(db, direct_requester, requesting_user, &source).await?;
        let source_mapped_sql = map_sql(
            query.to_owned(),
            entity_name,
            &source,
            &info_map_lookup,
            permission,
        )?;
        queries.push((
            source.id,
            Query {
                sql: source_mapped_sql.to_string(),
                return_schema: raw_request.return_arrow_schema.clone(),
            },
        ));
    }

    Ok(queries)
}

/// Looks up relevant [SourcePermission]s for the requesting [User] and
/// [Relay] (if query not recieved directly by a [User]). The
/// source permissions are combined into a single access policy by
/// taking the union of the default policy with the intersection
/// of the [User] and [Relay] policies. See [SourcePermission] docs
/// for more details on how union and intersection of policies are defined.
async fn evaluate_permission_policies(
    db: &mut PgDb<'_>,
    direct_requester: &Requester,
    requesting_user: &User,
    source: &DataSource,
) -> Result<SourcePermission> {
    debug!("Evaluating permission policies for direct_requester {direct_requester:?} and user: {requesting_user:?}");
    let default_permission = db.get_default_source_permission(&source.id).await?;
    let (user_permission, relay_permission) = match direct_requester {
        Requester::User(user) => {
            let user_permission = db
                .get_user_source_permission(&user.x509_sha256, &source.id)
                .await?;
            (user_permission, None)
        }
        Requester::Relay(relay) => {
            let user_permission = db
                .get_user_source_permission(&requesting_user.x509_sha256, &source.id)
                .await?;

            let relay_permission = db
                .get_relay_source_permission(&relay.id, &source.id)
                .await?;
            (user_permission, relay_permission)
        }
    };

    debug!(
        "Got default permission: {:?}, user permission: {:?}, and relay permission: {:?}",
        default_permission, user_permission, relay_permission
    );

    let permission = match (user_permission, relay_permission) {
        (Some(u), Some(r)) => default_permission
            .source_permission
            .union(&u.source_permission.intersection(&r.source_permission)),
        (None, Some(r)) => default_permission
            .source_permission
            .union(&r.source_permission),
        (Some(u), None) => default_permission
            .source_permission
            .union(&u.source_permission),
        (None, None) => default_permission.source_permission,
    };

    debug!("Resolved to {:?}", permission);

    Ok(permission)
}

/// Converts a [RawQueryRequest] received locally to a [RawQueryRequest] for each
/// peered remote relay.
pub async fn request_to_remote_requests(
    db: &mut PgDb<'_>,
    raw_request: &RawQueryRequest,
    query: &Statement,
    entity_name: &str,
    request_uuid: &Uuid,
    originating_relay: Relay,
    requesting_user: User,
) -> Result<Vec<(Uuid, RawQueryRequest)>> {
    let sources = db
        .get_remote_mappings_by_entity_names(vec![entity_name])
        .await?;

    let mut remote_query_requests = Vec::with_capacity(sources.len());
    for (relay, mappings) in sources.iter() {
        debug!("Processing remote requests for peer relay {relay:?}");

        let entity_map = &mappings
            .first()
            .ok_or(MeshError::InvalidQuery(format!(
                "No mappings found for relay {} and entity {entity_name}",
                relay.id
            )))?
            .2;

        let mut info_map_lookup = HashMap::with_capacity(mappings.len());
        for (_, info, _, map) in mappings.iter() {
            if info_map_lookup.insert(info.name.as_str(), map).is_some() {
                return Err(MeshError::InvalidQuery(format!(
                    "Found duplicate mapping for info {}",
                    info.name
                )));
            }
        }

        let mapped_query =
            map_remote_request(query.to_owned(), entity_name, entity_map, &info_map_lookup)?;

        remote_query_requests.push((
            relay.id,
            RawQueryRequest {
                sql: mapped_query.to_string(),
                request_uuid: Some(*request_uuid),
                requesting_user: Some(requesting_user.clone()),
                originating_relay: Some(originating_relay.clone()),
                originating_task_id: raw_request.originating_task_id,
                return_arrow_schema: raw_request.return_arrow_schema.clone(),
            },
        ))
    }

    Ok(remote_query_requests)
}
