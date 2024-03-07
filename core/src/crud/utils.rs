use std::collections::HashMap;
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaBuilder, SchemaRef};

use crate::model::entity::Entity;
use crate::model::{
    data_stores::{DataConnection, DataField, DataSource},
    entity::Information,
    mappings::{Mapping, RemoteEntityMapping, RemoteInfoMapping},
    relay::Relay,
};

type CorrelatedMappings = (
    DataConnection,
    DataSource,
    Entity,
    Information,
    DataField,
    Mapping,
);
type SourceSpecificMappings = (Entity, Information, DataField, Mapping);

type CorrelatedRemoteMappings = (
    Relay,
    Entity,
    Information,
    RemoteEntityMapping,
    RemoteInfoMapping,
);
type RelaySpecificMappings = (Entity, Information, RemoteEntityMapping, RemoteInfoMapping);

pub(crate) fn joined_rows_to_map(
    rows: Vec<CorrelatedMappings>,
) -> HashMap<(DataConnection, DataSource), Vec<SourceSpecificMappings>> {
    let mut outer_map: HashMap<(DataConnection, DataSource), Vec<SourceSpecificMappings>> =
        HashMap::new();
    for (con, source, entity, info, field, map) in rows {
        let key_tuple = (con, source);
        match outer_map.get_mut(&key_tuple) {
            Some(inner_vec) => {
                inner_vec.push((entity, info, field, map));
            }
            None => {
                outer_map.insert(key_tuple, vec![(entity, info, field, map)]);
            }
        }
    }
    outer_map
}

pub(crate) fn joined_rows_to_map_remote(
    rows: Vec<CorrelatedRemoteMappings>,
) -> HashMap<Relay, Vec<RelaySpecificMappings>> {
    let mut outer_map: HashMap<Relay, Vec<RelaySpecificMappings>> = HashMap::new();
    for (relay, entity, info, entity_map, info_map) in rows {
        match outer_map.get_mut(&relay) {
            Some(inner_vec) => {
                inner_vec.push((entity, info, entity_map, info_map));
            }
            None => {
                outer_map.insert(relay, vec![(entity, info, entity_map, info_map)]);
            }
        }
    }
    outer_map
}
