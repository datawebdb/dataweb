use std::sync::Arc;

use actix_web::{get, post, web, HttpRequest, HttpResponse, Responder};

use mesh::error::MeshError;

use mesh::execute::utils::{
    create_query_request, map_and_create_local_tasks, map_and_create_remote_tasks,
    verify_query_origination_information,
};
use mesh::execute::validation::validate_sql_template;

use tracing::{debug, info};

use crate::error::Result;
use crate::utils::{count_task_status, parse_certs_from_req, stream_all_task_results};
use crate::DbPool;
use mesh::crud::PgDb;
use mesh::execute::result_manager::ResultManager;

use mesh::messaging::{
    initialize_producer, GenericMessage, MessageBrokerOptions, QueryTaskMessage,
};

use mesh::model::query::RawQueryRequest;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct SubmitQueryResponse {
    id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
struct GetQueryResultResponse {
    data: Value,
    status: GetQueryStatus,
}

#[derive(Serialize, Deserialize, Debug)]
struct GetQueryStatus {
    request_id: Uuid,
    message: String,
    complete: usize,
    failed: usize,
    in_progress: usize,
}

#[derive(Deserialize)]
struct GetQueryOptions {
    allow_partial: Option<bool>,
    status_only: Option<bool>,
}

#[get("/query/{request_id}")]
async fn get_query_results(
    pool: web::Data<DbPool>,
    result_manager: web::Data<Arc<ResultManager>>,
    local_fingerprint: web::Data<Arc<String>>,
    client_cert_header: web::Data<Option<String>>,
    request_id: web::Path<Uuid>,
    options: web::Query<GetQueryOptions>,
    req: HttpRequest,
) -> Result<impl Responder> {
    let (fingerprint, subject_dn, issuer_dn) =
        parse_certs_from_req(req, client_cert_header.as_ref())?;

    info!(
        "Got new query request from: subject: {}, issuer: {}, fingerprint: {}",
        subject_dn, issuer_dn, fingerprint
    );

    let request_id = request_id.into_inner();
    let allow_partial = options.allow_partial.unwrap_or(false);
    let status_only = options.status_only.unwrap_or(false);

    let mut db = PgDb::try_from_pool(&pool).await?;

    let (request, tasks, remote_tasks) = match db.get_query_request(request_id).await? {
        Some(r) => r,
        None => {
            return Ok(
                HttpResponse::BadRequest().json(format!("No query exists with id {request_id}"))
            )
        }
    };

    // Access denied and no query exists intentionally give same response to prevent
    // brute forcing valid Uuids.
    match request.origin_info.origin_user {
        Some(origin_user) => {
            let retreiving_user = db.get_user_by_x509_fingerprint(&fingerprint).await?;
            if origin_user != retreiving_user {
                return Ok(HttpResponse::BadRequest()
                    .json(format!("No query exists with id {request_id}")));
            }
        }
        None => {
            return Ok(
                HttpResponse::BadRequest().json(format!("No query exists with id {request_id}"))
            )
        }
    }

    let flights = db.get_all_flight_streams(&remote_tasks).await?;
    let (complete, failed, in_progress) = count_task_status(&tasks, &flights);

    if !allow_partial && failed > 0 {
        let status = GetQueryStatus{
            request_id,
            message: format!("Some tasks have failed for {request_id}! Pass allow_partial=true for partial results or try query again."),
            complete,
            failed,
            in_progress
        };
        return Ok(HttpResponse::Ok().json(status));
    } else if !allow_partial && in_progress > 0 {
        let status = GetQueryStatus{
            request_id,
            message: format!("Some tasks are still in progress for {request_id}! Pass allow_partial=true for partial results or try to retrieve again later."),
            complete,
            failed,
            in_progress
        };
        return Ok(HttpResponse::Ok().json(status));
    } else if status_only {
        let status = GetQueryStatus {
            request_id,
            message: "Pass status_only=false to collect result when complete.".to_string(),
            complete,
            failed,
            in_progress,
        };
        return Ok(HttpResponse::Ok().json(status));
    }

    stream_all_task_results(
        &mut db,
        local_fingerprint.as_ref(),
        result_manager.as_ref(),
        tasks,
        flights,
    )
    .await
}

#[post("/query")]
async fn query(
    pool: web::Data<DbPool>,
    message_options: web::Data<MessageBrokerOptions>,
    local_fingerprint: web::Data<Arc<String>>,
    client_cert_header: web::Data<Option<String>>,
    query: web::Json<RawQueryRequest>,
    req: HttpRequest,
) -> Result<impl Responder> {
    let (fingerprint, subject_dn, issuer_dn) =
        parse_certs_from_req(req, client_cert_header.as_ref())?;
    info!(
        "Got new query request from: subject: {}, issuer: {}, fingerprint: {}",
        subject_dn, issuer_dn, fingerprint
    );
    let mut db = PgDb::try_from_pool(&pool).await?;

    let (direct_requester, requesting_user, originating_relay) =
        verify_query_origination_information(
            &query,
            &mut db,
            fingerprint,
            subject_dn,
            issuer_dn,
            local_fingerprint.as_ref(),
        )
        .await?;

    debug!("requesting_user: {requesting_user:?}, originating_relay: {originating_relay:?}");

    // It is possible that two requests bypass this check around the same time. This is OK as the database will later
    // raise a Unique contraint violation error. This check is only for efficiency, the database will always ensure correctness.
    match &query.request_uuid {
        Some(id) => match db.check_if_request_already_received(id).await {
            Ok(request) => {
                info!("Request id {id} already processed! Returning succesful response with no further action taken.");
                return Ok(HttpResponse::Ok().json(SubmitQueryResponse { id: request.id }));
            }
            Err(e) => debug!("Did not find already existing request with error: {e}"),
        },
        None => (),
    }

    debug!("Checking if sql template is valid...");
    validate_sql_template(&query.0)?;

    debug!("Creating QueryRequest");
    let request = match create_query_request(
        &query,
        &mut db,
        &direct_requester,
        &requesting_user,
        &originating_relay,
    )
    .await
    {
        Ok(q) => q,
        Err(MeshError::DuplicateQueryRequest(q)) => {
            info!(
                "Request id {} already processed! Returning succesful \
            response with no further action taken.",
                q.originator_request_id
            );
            return Ok(HttpResponse::Ok().json(SubmitQueryResponse { id: q.id }));
        }
        Err(e) => Err(e)?,
    };

    debug!("Mapping QueryRequest to local queries");
    let created_tasks = map_and_create_local_tasks(
        &query,
        &request,
        &mut db,
        &direct_requester,
        &requesting_user,
    )
    .await?;

    debug!("Mapping QueryRequest to remote queries");
    let created_remote_tasks = map_and_create_remote_tasks(
        &query,
        &request,
        &mut db,
        requesting_user,
        originating_relay,
    )
    .await?;

    debug!("Sending messages to QueryRunner");
    let mut producer = initialize_producer(&message_options).await?;
    for task in created_tasks {
        producer
            .send_message(&GenericMessage::LocalQueryTask(QueryTaskMessage {
                id: task.id,
            }))
            .await?;
    }

    for remote_task in created_remote_tasks {
        producer
            .send_message(&GenericMessage::RemoteQueryTask(QueryTaskMessage {
                id: remote_task.id,
            }))
            .await?;
    }

    debug!(
        "Successfully processed query with uuid {}!",
        request.originator_request_id
    );
    Ok(HttpResponse::Ok().json(SubmitQueryResponse { id: request.id }))
}
