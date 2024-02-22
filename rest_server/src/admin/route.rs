use std::sync::Arc;

use crate::admin::utils::process_config_obj;
use crate::error::Result;
use actix_web::{post, web, HttpRequest, HttpResponse, Responder};
use mesh::crud::PgDb;
use mesh::model::config_obj::ConfigObject;
use tracing::info;

use crate::utils::parse_certs_from_req;
use crate::DbPool;

#[post("/admin")]
async fn get_query_results(
    pool: web::Data<DbPool>,
    _local_fingerprint: web::Data<Arc<String>>,
    client_cert_header: web::Data<Option<String>>,
    config_obj: web::Json<ConfigObject>,
    req: HttpRequest,
) -> Result<impl Responder> {
    let (fingerprint, subject_dn, issuer_dn) =
        parse_certs_from_req(req, client_cert_header.as_ref())?;

    info!(
        "Got new query request from: subject: {}, issuer: {}, fingerprint: {}",
        subject_dn, issuer_dn, fingerprint
    );

    let mut db = PgDb::try_from_pool(&pool).await?;

    process_config_obj(&mut db, config_obj.0).await?;

    Ok(HttpResponse::Ok())
}
