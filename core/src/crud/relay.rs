use crate::model::access_control::{RelaySourcePermission, SourcePermission};
use crate::model::relay::Relay;
use crate::{error::Result, model::relay::NewRelay};

use crate::schema;
use diesel::{insert_into, prelude::*};
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use super::PgDb;

impl<'a> PgDb<'a> {
    /// Creates a new [Relay] or updates it based on the x509_pem value
    pub async fn upsert_relay(&mut self, val: &NewRelay) -> Result<Relay> {
        use schema::relays::dsl::*;
        insert_into(relays)
            .values(val)
            .on_conflict(name)
            .do_update()
            .set(val)
            .execute(&mut self.con)
            .await?;
        self.get_relay_by_x509_fingerprint(&val.x509_sha256).await
    }

    pub async fn get_relay_by_x509_fingerprint(&mut self, x509_sha256_val: &str) -> Result<Relay> {
        use schema::relays::dsl::*;
        Ok(relays
            .filter(x509_sha256.eq(x509_sha256_val))
            .select(Relay::as_select())
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn get_relay_by_name(&mut self, name_val: &str) -> Result<Relay> {
        use schema::relays::dsl::*;
        Ok(relays
            .filter(name.eq(name_val))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn get_relay_by_id(&mut self, id_val: &Uuid) -> Result<Relay> {
        use schema::relays::dsl::*;
        Ok(relays
            .filter(id.eq(id_val))
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn upsert_relay_source_permission(
        &mut self,
        relay_id_val: &Uuid,
        data_source_id_val: &Uuid,
        source_permission_val: &SourcePermission,
    ) -> Result<()> {
        use schema::relay_source_permission::dsl::*;
        let record = (
            relay_id.eq(relay_id_val),
            data_source_id.eq(data_source_id_val),
            source_permission.eq(source_permission_val),
        );
        insert_into(relay_source_permission)
            .values(&record)
            .on_conflict((data_source_id, relay_id))
            .do_update()
            .set(record)
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    pub async fn get_relay_source_permission(
        &mut self,
        relay_id_val: &Uuid,
        source_id_val: &Uuid,
    ) -> Result<Option<RelaySourcePermission>> {
        use schema::relay_source_permission::dsl::*;
        Ok(relay_source_permission
            .filter(
                relay_id
                    .eq(relay_id_val)
                    .and(data_source_id.eq(source_id_val)),
            )
            .get_result(&mut self.con)
            .await
            .optional()?)
    }
}
