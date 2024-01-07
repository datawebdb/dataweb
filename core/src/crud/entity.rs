use std::collections::HashMap;

use crate::model::entity::{Information, NewInformation};
use crate::{error::Result, model::entity::Entity};

use crate::schema;
use diesel::{insert_into, prelude::*};
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use super::PgDb;

impl<'a> PgDb<'a> {
    pub async fn create_entity(&mut self, name_val: &str) -> Result<Entity> {
        use schema::entities::dsl::*;
        Ok(insert_into(entities)
            .values(name.eq(name_val))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn get_entity(&mut self, name_val: &str) -> Result<Entity> {
        use schema::entities::dsl::*;
        Ok(entities
            .filter(name.eq(name_val))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn create_entity_if_not_exist(&mut self, name_val: &str) -> Result<Entity> {
        use schema::entities::dsl::*;
        insert_into(entities)
            .values(name.eq(name_val))
            .on_conflict(name)
            .do_nothing()
            .execute(&mut self.con)
            .await?;
        Ok(entities
            .filter(name.eq(name_val))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn create_information(
        &mut self,
        vals: &Vec<NewInformation>,
    ) -> Result<Vec<Information>> {
        use schema::information::dsl::*;
        Ok(insert_into(information)
            .values(vals)
            .get_results(&mut self.con)
            .await?)
    }

    pub async fn upsert_information(&mut self, val: &NewInformation) -> Result<()> {
        use schema::information::dsl::*;
        insert_into(information)
            .values(val)
            .on_conflict((entity_id, name))
            .do_update()
            .set(val)
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    /// Get relevant information for a given [Entity] based on its id
    pub async fn get_information_for_entity(
        &mut self,
        entity_id_val: Uuid,
    ) -> Result<Vec<Information>> {
        use schema::information::dsl::*;
        Ok(information
            .filter(entity_id.eq(entity_id_val))
            .select(Information::as_select())
            .get_results(&mut self.con)
            .await?)
    }

    pub async fn get_information(
        &mut self,
        info_name: &str,
        entity_id_val: &Uuid,
    ) -> Result<Information> {
        use schema::information::dsl::*;
        Ok(information
            .filter(entity_id.eq(entity_id_val).and(name.eq(info_name)))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn get_all_information(&mut self) -> Result<HashMap<String, Vec<Information>>> {
        use schema::entities::dsl as entity;
        use schema::information::dsl as information;

        let rows: Vec<(Entity, Information)> = information::information
            .inner_join(entity::entities)
            .select((Entity::as_select(), Information::as_select()))
            .get_results(&mut self.con)
            .await?;

        let mut out: HashMap<String, Vec<Information>> = HashMap::new();
        for (e, i) in rows {
            match out.get_mut(&e.name) {
                Some(v) => v.push(i),
                None => {
                    out.insert(e.name, vec![i]);
                }
            }
        }

        Ok(out)
    }
}
