use crate::model::access_control::{SourcePermission, UserSourcePermission};
use crate::model::user::User;
use crate::{error::Result, model::user::NewUser};

use crate::schema;
use diesel::{insert_into, prelude::*};
use diesel_async::RunQueryDsl;
use uuid::Uuid;

use super::PgDb;

impl<'a> PgDb<'a> {
    pub async fn upsert_user_by_fingerprint(&mut self, val: &NewUser) -> Result<User> {
        use schema::users::dsl::*;
        insert_into(users)
            .values(val)
            .on_conflict(x509_sha256)
            .do_update()
            .set(val)
            .execute(&mut self.con)
            .await?;

        Ok(users
            .filter(x509_sha256.eq(&val.x509_sha256))
            .select(User::as_select())
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn get_user_by_x509_fingerprint(&mut self, x509_sha256_val: &str) -> Result<User> {
        use schema::users::dsl::*;
        Ok(users
            .filter(x509_sha256.eq(x509_sha256_val))
            .select(User::as_select())
            .get_result(&mut self.con)
            .await?)
    }

    pub async fn upsert_user_source_permission(
        &mut self,
        user_id_val: &Uuid,
        data_source_id_val: &Uuid,
        source_permission_val: &SourcePermission,
    ) -> Result<()> {
        use schema::user_source_permission::dsl::*;
        let record = (
            user_id.eq(user_id_val),
            data_source_id.eq(data_source_id_val),
            source_permission.eq(source_permission_val),
        );
        insert_into(user_source_permission)
            .values(&record)
            .on_conflict((data_source_id, user_id))
            .do_update()
            .set(record)
            .execute(&mut self.con)
            .await?;
        Ok(())
    }

    pub async fn get_user_source_permission(
        &mut self,
        user_fingerprint: &String,
        source_id_val: &Uuid,
    ) -> Result<Option<UserSourcePermission>> {
        use schema::user_source_permission::dsl::*;
        use schema::users::dsl as users;
        Ok(user_source_permission
            .inner_join(users::users)
            .filter(
                users::x509_sha256
                    .eq(user_fingerprint)
                    .and(data_source_id.eq(source_id_val)),
            )
            .select(UserSourcePermission::as_select())
            .get_result(&mut self.con)
            .await
            .optional()?)
    }
}
