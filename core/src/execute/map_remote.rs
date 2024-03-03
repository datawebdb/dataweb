use std::collections::HashMap;

use crate::error::Result;
use crate::model::data_stores::DataField;
use crate::model::entity::{Entity, Information};
use crate::model::mappings::{RemoteEntityMapping, RemoteInfoMapping};
use crate::model::query::{
    default_scope, InfoSubstitution, OriginatorEntityMapping, OriginatorInfoMapping,
    OriginatorMappings, RawQueryRequest, ScopedOriginatorMappings,
};
use crate::model::relay::Relay;
use crate::model::user::User;
use crate::{
    error::MeshError,
    model::query::{SourceSubstitution, SubstitutionBlocks},
};

use datafusion::sql::sqlparser::ast::Statement;
use itertools::Itertools;
use tracing::debug;
use uuid::Uuid;

use super::parse_utils::{
    apply_col_iden_mapping, parse_sql_as_table_factor, substitute_table_factor,
};
use super::utils::compose_derived_source_substitution;

/// Substitutes appropriate [Entity][crate::model::entity::Entity] and
/// [Information][crate::model::entity::Information] names for a specific remote relay
/// for a [SubstitutionBlocks] object sent to the local relay. Also applies relevant
/// [Transformations][crate::model::mappings::Transformation] to the request SQL.
pub(crate) fn map_remote_request(
    mut statement: Statement,
    entity_name: &str,
    entity_map: &RemoteEntityMapping,
    info_map_lookup: &HashMap<&str, &RemoteInfoMapping>,
) -> Result<Statement> {
    apply_source_substitutions(&mut statement, entity_map)?;

    apply_info_substitutions(&mut statement, &info_map_lookup, entity_name)?;

    Ok(statement)
}

fn apply_source_substitutions(
    statement: &mut Statement,
    source: &RemoteEntityMapping,
) -> Result<()> {
    let source_sql = &source.sql;
    let remote_table = parse_sql_as_table_factor(source_sql)?;

    substitute_table_factor(statement, remote_table)?;
    Ok(())
}

fn apply_info_substitutions(
    statement: &mut Statement,
    info_map_lookup: &HashMap<&str, &RemoteInfoMapping>,
    entity_name: &str,
) -> Result<()> {
    let filtered_map = info_map_lookup
        .iter()
        .map(|(info, map)| {
            let transform = &map.transformation;
            (
                *info,
                transform
                    .other_to_local_info
                    .replace(&transform.replace_from, &map.info_mapped_name),
            )
        })
        .collect::<HashMap<_, _>>();

    apply_col_iden_mapping(statement, &filtered_map, entity_name)?;

    Ok(())
}

#[cfg(test)]
mod tests {}
