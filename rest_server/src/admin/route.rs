use std::sync::Arc;

use crate::admin::utils::process_config_obj;
use crate::error::{RelayError, Result};
use actix_web::{post, web, HttpRequest, HttpResponse, Responder};
use mesh::crud::PgDb;
use mesh::model::config_commands::ResolvedConfigCommand;
use tracing::info;

use crate::utils::parse_certs_from_req;
use crate::DbPool;

#[post("/admin/apply")]
async fn apply(
    pool: web::Data<DbPool>,
    _local_fingerprint: web::Data<Arc<String>>,
    client_cert_header: web::Data<Option<String>>,
    config_obj: web::Json<ResolvedConfigCommand>,
    req: HttpRequest,
) -> Result<impl Responder> {
    let (fingerprint, subject_dn, issuer_dn) =
        parse_certs_from_req(req, client_cert_header.as_ref())?;

    info!(
        "Got new query request from: subject: {}, issuer: {}, fingerprint: {}",
        subject_dn, issuer_dn, fingerprint
    );

    let mut db = PgDb::try_from_pool(&pool).await?;
    let user = db.get_user_by_x509_fingerprint(&fingerprint).await?;
    if !user.attributes.is_admin {
        return Err(RelayError::new(
            "User is unauthorized for adminstrative actions!",
        ));
    }

    process_config_obj(&mut db, config_obj.0.config_object).await?;

    Ok(HttpResponse::Ok())
}
