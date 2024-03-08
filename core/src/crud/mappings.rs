use std::collections::HashMap;

use crate::{
    crud::utils::{joined_rows_to_map, joined_rows_to_map_remote},
    model::{
        data_stores::{DataConnection, DataField, DataSource},
        entity::Information,
        mappings::{Mapping, NewRemoteEntityMapping, RemoteEntityMapping, RemoteInfoMapping},
        relay::Relay,
    }, schema::remote_info_mapping::info_mapped_name,
};
use crate::{error::Result, model::entity::Entity};

use crate::schema;
use diesel::{insert_into, prelude::*};
use diesel_async::RunQueryDsl;

use uuid::Uuid;

use super::PgDb;

impl<'a> PgDb<'a> {
    pub async fn upsert_local_mapping(&mut self, val: &Mapping) -> Result<()> {
        use schema::field_mappings::dsl::*;
        insert_into(field_mappings)
            .values(val)
            .on_conflict((information_id, data_field_id))
            .do_update()
            .set(val)
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    pub async fn get_remote_entity_mapping(
        &mut self,
        relay_id_val: &Uuid,
        entity_id_val: &Uuid,
        remote_entity_name_val: &String,
    ) -> Result<RemoteEntityMapping> {
        use schema::remote_entity_mapping::dsl::*;
        Ok(remote_entity_mapping
            .filter(
                relay_id.eq(relay_id_val).and(
                    entity_id
                        .eq(entity_id_val)
                        .and(remote_entity_name.eq(remote_entity_name_val)),
                ),
            )
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn upsert_remote_entity_mapping(
        &mut self,
        val: &NewRemoteEntityMapping,
    ) -> Result<RemoteEntityMapping> {
        use schema::remote_entity_mapping::dsl::*;
        insert_into(remote_entity_mapping)
            .values(val)
            .on_conflict((relay_id, entity_id, remote_entity_name))
            .do_update()
            .set(val)
            .execute(&mut self.con)
            .await?;
        self.get_remote_entity_mapping(&val.relay_id, &val.entity_id, &val.remote_entity_name)
            .await
    }

    pub async fn upsert_remote_info_mapping(&mut self, val: &RemoteInfoMapping) -> Result<()> {
        use schema::remote_info_mapping::dsl::*;
        insert_into(remote_info_mapping)
            .values(val)
            .on_conflict((remote_entity_mapping_id, information_id))
            .do_update()
            .set(val)
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    pub async fn get_mappings_by_entity_names(
        &mut self,
        entity_name_vals: Vec<&str>,
    ) -> Result<HashMap<(DataConnection, DataSource), Vec<(Entity, Information, DataField, Mapping)>>>
    {
        use schema::data_connection::dsl as conn;
        use schema::data_field::dsl as field;
        use schema::data_source::dsl as source;
        use schema::field_mappings::dsl as map;

        use schema::entities::dsl as entity;
        use schema::information::dsl as information;

        let rows: Vec<(
            DataConnection,
            DataSource,
            Entity,
            Information,
            DataField,
            Mapping,
        )> = schema::information::table
            .inner_join(entity::entities)
            .inner_join(map::field_mappings.inner_join(
                field::data_field.inner_join(source::data_source.inner_join(conn::data_connection)),
            ))
            .filter(
                entity::name
                    .eq_any(entity_name_vals)
                    .and(information::id.eq(map::information_id)),
            )
            .select((
                DataConnection::as_select(),
                DataSource::as_select(),
                Entity::as_select(),
                Information::as_select(),
                DataField::as_select(),
                Mapping::as_select(),
            ))
            .load(&mut self.con)
            .await?;

        Ok(joined_rows_to_map(rows))
    }

    pub async fn get_remote_mappings_by_entity_names(
        &mut self,
        entity_name_vals: Vec<&str>,
    ) -> Result<HashMap<Relay, Vec<(Entity, Information, RemoteEntityMapping, RemoteInfoMapping)>>>
    {
        use schema::entities::dsl as entity;
        use schema::information::dsl as information;
        use schema::relays::dsl as remote;
        use schema::remote_entity_mapping::dsl as entity_mapping;
        use schema::remote_info_mapping::dsl as info_mapping;

        let rows: Vec<(
            Relay,
            Entity,
            Information,
            RemoteEntityMapping,
            RemoteInfoMapping,
        )> = schema::information::table
            .inner_join(
                entity::entities.inner_join(
                    entity_mapping::remote_entity_mapping
                        .inner_join(remote::relays)
                        .inner_join(info_mapping::remote_info_mapping),
                ),
            )
            .filter(
                entity::name
                    .eq_any(entity_name_vals)
                    .and(information::id.eq(info_mapping::information_id)),
            )
            .select((
                Relay::as_select(),
                Entity::as_select(),
                Information::as_select(),
                RemoteEntityMapping::as_select(),
                RemoteInfoMapping::as_select(),
            ))
            .load(&mut self.con)
            .await?;

        Ok(joined_rows_to_map_remote(rows))
    }

    pub async fn get_mappings_by_source_ids(
        &mut self,
        source_ids: Vec<Uuid>,
    ) -> Result<HashMap<(DataConnection, DataSource), Vec<(Entity, Information, DataField, Mapping)>>>
    {
        use schema::data_connection::dsl as conn;
        use schema::data_field::dsl as field;
        use schema::data_source::dsl as source;
        use schema::entities::dsl as entity;
        use schema::field_mappings::dsl as map;

        let rows: Vec<(
            DataConnection,
            DataSource,
            Entity,
            Information,
            DataField,
            Mapping,
        )> = schema::information::table
            .inner_join(entity::entities)
            .inner_join(map::field_mappings.inner_join(
                field::data_field.inner_join(source::data_source.inner_join(conn::data_connection)),
            ))
            .filter(source::id.eq_any(source_ids))
            .select((
                DataConnection::as_select(),
                DataSource::as_select(),
                Entity::as_select(),
                Information::as_select(),
                DataField::as_select(),
                Mapping::as_select(),
            ))
            .load(&mut self.con)
            .await?;

        Ok(joined_rows_to_map(rows))
    }
}
