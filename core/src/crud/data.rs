use crate::error::Result;
use crate::model::access_control::{DefaultSourcePermission, SourcePermission};
use crate::model::data_stores::{
    options::ConnectionOptions, DataConnection, DataField, DataSource, NewDataField, NewDataSource,
};

use crate::schema::{self};
use diesel::{insert_into, prelude::*};
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use super::PgDb;

impl<'a> PgDb<'a> {
    pub async fn create_connection(
        &mut self,
        name_val: &str,
        connection_options_val: ConnectionOptions,
    ) -> Result<DataConnection> {
        use schema::data_connection::dsl::*;
        Ok(insert_into(data_connection)
            .values((
                name.eq(name_val),
                connection_options.eq(connection_options_val),
            ))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn get_connection(&mut self, name_val: &str) -> Result<DataConnection> {
        use schema::data_connection::dsl::*;
        Ok(data_connection
            .filter(name.eq(name_val))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn upsert_connection(
        &mut self,
        name_val: &str,
        connection_options_val: ConnectionOptions,
    ) -> Result<DataConnection> {
        use schema::data_connection::dsl::*;
        insert_into(data_connection)
            .values((
                name.eq(name_val),
                connection_options.eq(&connection_options_val),
            ))
            .on_conflict(name)
            .do_update()
            .set(connection_options.eq(&connection_options_val))
            .execute(&mut self.con)
            .await?;
        self.get_connection(name_val).await
    }

    pub async fn create_source(&mut self, vals: &Vec<NewDataSource>) -> Result<Vec<DataSource>> {
        use schema::data_source::dsl::*;
        Ok(insert_into(data_source)
            .values(vals)
            .get_results(&mut self.con)
            .await?)
    }

    pub async fn get_source(
        &mut self,
        name_val: &str,
        data_con_id_val: &Uuid,
    ) -> Result<DataSource> {
        use schema::data_source::dsl::*;
        Ok(data_source
            .filter(
                name.eq(name_val)
                    .and(data_connection_id.eq(data_con_id_val)),
            )
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn upsert_source(&mut self, val: &NewDataSource) -> Result<DataSource> {
        use schema::data_source::dsl::*;
        insert_into(data_source)
            .values(val)
            .on_conflict((data_connection_id, name))
            .do_update()
            .set(val)
            .execute(&mut self.con)
            .await?;
        self.get_source(&val.name, &val.data_connection_id).await
    }

    pub async fn create_field(&mut self, vals: &Vec<NewDataField>) -> Result<Vec<DataField>> {
        use schema::data_field::dsl::*;
        Ok(insert_into(data_field)
            .values(vals)
            .get_results(&mut self.con)
            .await?)
    }

    pub async fn get_field(
        &mut self,
        name_val: &str,
        data_source_id_val: &Uuid,
    ) -> Result<DataField> {
        use schema::data_field::dsl::*;
        Ok(data_field
            .filter(name.eq(name_val).and(data_source_id.eq(data_source_id_val)))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn upsert_field(&mut self, val: &NewDataField) -> Result<()> {
        use schema::data_field::dsl::*;
        insert_into(data_field)
            .values(val)
            .on_conflict((data_source_id, name))
            .do_update()
            .set(val)
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    pub async fn upsert_default_source_permission(
        &mut self,
        data_source_id_val: &Uuid,
        source_permission_val: &SourcePermission,
    ) -> Result<()> {
        use schema::default_source_permission::dsl::*;
        let record = (
            data_source_id.eq(data_source_id_val),
            source_permission.eq(source_permission_val),
        );
        insert_into(default_source_permission)
            .values(&record)
            .on_conflict(data_source_id)
            .do_update()
            .set(record)
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    pub async fn get_default_source_permission(
        &mut self,
        source_id_val: &Uuid,
    ) -> Result<DefaultSourcePermission> {
        use schema::default_source_permission::dsl::*;
        Ok(default_source_permission
            .filter(data_source_id.eq(source_id_val))
            .get_result(&mut self.con)
            .await?)
    }
}
