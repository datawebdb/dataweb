pub mod data_stores;
mod map_local;
mod map_remote;
pub(crate) mod parse_utils;
pub(crate) mod planning;
pub mod result_manager;
pub mod utils;
pub mod validation;

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use crate::error::Result;
use crate::model::access_control::SourcePermission;
use crate::model::data_stores::DataSource;
use crate::model::mappings::{RemoteEntityMapping, RemoteInfoMapping};
use crate::model::query::RawQueryRequest;
use crate::model::relay::Relay;
use crate::model::user::User;
use crate::{
    crud::PgDb,
    error::MeshError,
    model::query::{Query, SourceSubstitution},
};

use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::ast::{visit_relations, TableFactor, VisitMut, VisitorMut};
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

/// Maps Local [Entity][crate::model::entity::Entity] names to Remote Entity names and the corresponding map of
/// local [Information][crate::model::entity::Information] names to remote Information names.
pub type LocaltoRemoteNameHashMap<'a> = HashMap<
    &'a str,
    (
        &'a RemoteEntityMapping,
        HashMap<&'a str, &'a RemoteInfoMapping>,
    ),
>;

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
    raw_request: &RawQueryRequest,
    direct_requester: &Requester,
    requesting_user: &User,
) -> Result<Vec<(Uuid, Query)>> {
    let mut entities = vec![];
    visit_relations(query, |relation| {
        let entity = relation.to_string();
        if !entities.contains(&entity) {
            entities.push(entity);
        }
        std::ops::ControlFlow::<()>::Continue(())
    });

    if entities.len() != 1 {
        return Err(MeshError::InvalidQuery(
            "There must be exactly one entity per query.".to_string(),
        ));
    }

    let sources = db.get_mappings_by_entity_names(entities).await?;

    let mut queries = Vec::with_capacity(sources.len());
    for ((con, source), mappings) in sources {
        let permission =
            evaluate_permission_policies(db, direct_requester, requesting_user, &source).await?;
        let source_mapped_sql = map_sql(query.to_owned(), &con, &source, &mappings, permission)?;
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
    request_uuid: &Uuid,
    originating_relay: Relay,
    requesting_user: User,
) -> Result<Vec<(Uuid, RawQueryRequest)>> {
    let blocks = &raw_request.substitution_blocks;

    let sources = match &blocks
        .source_substitutions
        .iter()
        .next()
        .expect("There must be at least one source substitution!")
        .1
    {
        // Get mappings in each case, there can only be one case due to constraints above
        SourceSubstitution::AllSourcesWith(entities) => {
            let mut all_entities = HashSet::new();
            for e in entities {
                all_entities.insert(e.as_str());
            }
            db.get_remote_mappings_by_entity_names(Vec::from_iter(all_entities))
                .await?
        }
        // Explicit source lists currently need to be sent to the relevant relay directly
        SourceSubstitution::SourceList(_raw_sources) => HashMap::new(),
    };

    let mut remote_query_requests = Vec::with_capacity(sources.len());
    for (relay, mappings) in sources.iter() {
        debug!("Processing remote requests for peer relay {relay:?}");
        let mut name_map: LocaltoRemoteNameHashMap = HashMap::new();
        for (entity, info, entity_map, info_map) in mappings {
            debug!("Got entity {} and info {}", entity.name, info.name);
            debug!("Got {entity_map:?} and {info_map:?}");
            match name_map.get_mut(entity.name.as_str()) {
                Some(v) => {
                    if v.0.remote_entity_name == entity_map.remote_entity_name.as_str() {
                        let info_namemap = &mut v.1;
                        match info_namemap.get(info.name.as_str()) {
                            Some(_) => {
                                return Err(MeshError::InvalidQuery(format!(
                                    "Found duplicate info name {} for entity {} and relay {}!",
                                    info.name, entity.name, relay.id
                                )))
                            }
                            None => {
                                info_namemap.insert(info.name.as_str(), info_map);
                            }
                        }
                    } else {
                        return Err(MeshError::InvalidQuery(format!(
                            "Found duplicate and conflicting entity mappings! \
                        Local name: {}, Remote names: {} and {}",
                            entity.name, v.0.remote_entity_name, entity_map.remote_entity_name
                        )));
                    }
                }
                None => {
                    let mut info_namemap = HashMap::new();
                    info_namemap.insert(info.name.as_str(), info_map);
                    name_map.insert(&entity.name, (&entity_map, info_namemap));
                }
            };
        }
        debug!("Got name map {name_map:?}");
        remote_query_requests.push((
            relay.id,
            map_remote_request(
                raw_request,
                relay,
                Some(originating_relay.clone()),
                &requesting_user,
                request_uuid,
                &name_map,
            )?,
        ))
    }

    Ok(remote_query_requests)
}
