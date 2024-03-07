use arrow::ipc::convert::try_schema_from_flatbuffer_bytes;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::utils::flight_data_to_arrow_batch;
use arrow_flight::{FlightClient, FlightEndpoint};
use arrow_schema::{Field, Schema};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::sql::sqlparser::ast::Statement;
use diesel_async::pooled_connection::bb8::Pool;

use diesel_async::AsyncPgConnection;
use futures::{StreamExt, TryStreamExt};

use mesh::crud::PgDb;

use mesh::error::MeshError;
use mesh::execute::data_stores::try_connect;
use mesh::execute::result_manager::ResultManager;

use mesh::execute::request_to_remote_requests;
use mesh::execute::utils::{
    create_query_request, map_and_create_local_tasks, validate_sql_and_logical_round_trip,
    verify_query_origination_information,
};

use mesh::model::data_stores::{DataConnection, DataSource};
use mesh::model::query::{
    FlightStreamStatus, NewFlightStream, QueryRequest, QueryTask, RawQueryRequest,
};
use mesh::model::relay::Relay;
use mesh::model::user::User;
use mesh::pki::{parse_certificate, parse_urlencoded_pemstr};

use std::collections::HashMap;

use tokio::task::JoinSet;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};
use tracing::{debug, error, info, warn};

use uuid::Uuid;

use std::sync::Arc;

use futures::stream::BoxStream;

use tonic::{Request, Response, Status, Streaming};

use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};

use serde::{Deserialize, Serialize};
use tonic::codegen::Bytes;

/// Used as a [Ticket], and can also pass metadata to flight clients
#[derive(Serialize, Deserialize)]
struct FlightInfoTicket {
    data_source_id: Uuid,
    task_id: Uuid,
}

/// Generic function to extract client certificate information from any [Request].
/// This function relies on direct access to the client's certificate in the mTLS
/// handshake, and so cannot be used when the Relay is behind a reverse proxy which
/// terminates TLS. In that case have the proxy pass a header with the client's cert
/// and use extract_certs_header function instead.
/// See [parse_certificate] for more information about the return values.
fn extract_certs_direct_tls<T>(request: &Request<T>) -> Result<(String, String, String), Status> {
    let client_certs = request.peer_certs().ok_or(Status::permission_denied(
        "Expected client cert, found none",
    ))?;
    if client_certs.is_empty() {
        return Err(Status::permission_denied(
            "Expected client cert, found none",
        ));
    }

    let cert = &client_certs[0];
    let rustls_cert = rustls::Certificate(cert.to_owned().into_inner());
    let (fingerprint, subject_dn, issuer_dn) = parse_certificate(&rustls_cert)
        .map_err(|_e| Status::permission_denied("Found client cert, but unable to parse"))?;
    Ok((fingerprint, subject_dn, issuer_dn))
}

/// Extracts client's certificate from a header. This assumes there is a trusted reverse proxy upstream
/// which completes the mTLS handshake and forwards the validated cert. This method is only secure when
/// the only way to connect to the Relay is via the upstream reverse proxy.
/// See [parse_certificate] for more information about the return values.
fn extract_certs_header<T>(
    request: &Request<T>,
    header: &String,
) -> Result<(String, String, String), Status> {
    match request.metadata().get(header) {
        Some(value) => {
            let inner = value
                .to_str()
                .map_err(|_e| Status::unauthenticated("Unable to read client cert from header"))?;
            parse_urlencoded_pemstr(inner).map_err(|e| {
                Status::unauthenticated(format!(
                    "Cert header authentication failed with error: {e}"
                ))
            })
        }
        None => Err(Status::unauthenticated(
            "Unable to retrieve client certificate from header!",
        )),
    }
}

/// Generic function to extract client certificate information from any [Request]
/// See [parse_certificate] for more information about the return values.
fn extract_certs<T>(
    request: &Request<T>,
    client_cert_header: &Option<String>,
) -> Result<(String, String, String), Status> {
    match client_cert_header {
        Some(header) => extract_certs_header(request, header),
        None => extract_certs_direct_tls(request),
    }
}

#[derive(Clone)]
pub struct FlightRelay {
    pub db_pool: Pool<AsyncPgConnection>,
    pub result_manager: Arc<ResultManager>,
    /// x509 certificate used for authenticating as a client with other flight servers
    pub client_cert: Arc<Vec<u8>>,
    /// private key of the x509 client certificate
    pub client_key: Arc<Vec<u8>>,
    /// CAcert bundle used to verify other flight servers when making a request as a client
    pub ca_cert: Arc<Vec<u8>>,
    /// This is the x509 cert fingerprint of the local Relays client certificate.
    /// The local Relay will propagate this value with requests forwarded to remote
    /// Relays if the local Relay is the originator.
    pub local_fingerprint: Arc<String>,
    /// In direct_tls=false mode, the FlightRelay trusts the upstream server to pass the certificate of
    /// the client in this header. This is only secure when the only way to connect to the Relay is via
    /// the upstream reverse proxy which terminates TLS. In direct_tls=true mode, this is None and the Relay
    /// directly validates the user's certificate via mTLS handshake.
    pub client_cert_header: Option<String>,
}

impl FlightRelay {
    async fn get_flight_client(&self, relay: &Relay) -> Result<FlightClient, Status> {
        let channel = tonic::transport::Channel::from_shared(relay.flight_endpoint.clone())
            .map_err(|e| Status::from_error(Box::new(e)))?
            .tls_config(
                ClientTlsConfig::new()
                    .identity(Identity::from_pem(
                        self.client_cert.as_ref(),
                        self.client_key.as_ref(),
                    ))
                    .ca_certificate(Certificate::from_pem(self.ca_cert.as_ref())),
            )
            .map_err(|e| Status::from_error(Box::new(e)))?
            .connect()
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let svc_client = FlightServiceClient::new(channel);
        Ok(FlightClient::new_from_inner(svc_client))
    }

    /// Execute a local [QueryTask] and return a RecordBatch stream
    async fn execute_query_task(
        &self,
        con: DataConnection,
        source: DataSource,
        task: QueryTask,
    ) -> Result<SendableRecordBatchStream, Status> {
        let query = task.task;
        let mut runner = try_connect(con, source).await.map_err(|e| {
            error!("Execution error: {e}");
            Status::internal(format!(
                "An unexpected error occurred while processing local task {}",
                task.id
            ))
        })?;
        let rb_stream = runner.execute_stream(query).await.map_err(|e| {
            error!("Execution error: {e}");
            Status::internal(format!(
                "An unexpected error occurred while processing local task {}",
                task.id
            ))
        })?;

        Ok(rb_stream)
    }

    /// Creates an intial [FlightInfo] response including a [FlightEndpoint] for each
    /// relevant local [DataSource].
    async fn create_flight_info_response(
        &self,
        flight_descriptor: FlightDescriptor,
        db: &mut PgDb<'_>,
        created_tasks: Vec<QueryTask>,
    ) -> Result<FlightInfo, Status> {
        let mut response = FlightInfo::new()
            .try_with_schema(&Schema::new(Vec::<Field>::new()))
            .expect("encoding empty schema should never fail")
            .with_descriptor(flight_descriptor);

        // If there is relevant local data, add our own endpoint to the FlightInfo
        let local_relay = db
            .get_relay_by_x509_fingerprint(self.local_fingerprint.as_ref())
            .await
            .map_err(|e| {
                Status::internal(format!("Unable to get local relay info with error {e}"))
            })?;
        for task in created_tasks {
            let flight_info_ticket = FlightInfoTicket {
                data_source_id: task.data_source_id,
                task_id: task.id,
            };

            let ticket = serde_json::to_vec(&flight_info_ticket).map_err(|e| {
                error!("Unexpected error encoding flight_info_ticket as json {e}");
                Status::internal("Unexpected internal error")
            })?;
            response = response.with_endpoint(
                FlightEndpoint::new()
                    .with_ticket(Ticket::new(Into::<Bytes>::into(ticket)))
                    .with_location(&local_relay.flight_endpoint),
            )
        }

        Ok(response)
    }

    /// Queries remote relays which may have relevant data for additional flight endpoints
    /// which should be added to the response.
    async fn update_flight_info_response_from_remotes(
        &self,
        mut response: FlightInfo,
        db: &mut PgDb<'_>,
        raw_request: &RawQueryRequest,
        query: &Statement,
        entity_name: &str,
        request: &QueryRequest,
        originating_relay: Relay,
        requesting_user: User,
    ) -> Result<FlightInfo, Status> {
        debug!("Mapping QueryRequest to remote queries");
        let remote_requests = request_to_remote_requests(
            db,
            raw_request,
            query,
            entity_name,
            &request.originator_request_id,
            originating_relay,
            requesting_user,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut remote_tasks: JoinSet<Result<FlightInfo, Status>> = JoinSet::new();
        for (relay_id, mut remote_request) in remote_requests {
            // Assign a Uuid to this remote request and if no originating task id is set,
            // assign our remote task Uuid as originating_task_id.
            let id = Uuid::new_v4();
            if remote_request.originating_task_id.is_none() {
                remote_request.originating_task_id = Some(id);
            }
            let remote = db
                .get_relay_by_id(&relay_id)
                .await
                .map_err(|e| Status::from_error(Box::new(e)))?;
            debug!("Connecting to {}", remote.flight_endpoint);
            let mut client = match self.get_flight_client(&remote).await {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to connect to {remote:?} with error {e}");
                    continue;
                }
            };
            let descriptor = FlightDescriptor::new_cmd(
                serde_json::to_vec(&remote_request).map_err(|e| Status::from_error(Box::new(e)))?,
            );

            debug!("Spawning task to send remote_request: {remote_request:?}");
            remote_tasks.spawn(async move {
                debug!("Sending get_flight_info request");
                client
                    .get_flight_info(descriptor)
                    .await
                    .map_err(|e| Status::from_error(Box::new(e)))
            });
        }

        while let Some(result) = remote_tasks.join_next().await {
            match result {
                Ok(Ok(r)) => {
                    debug!("Got {} endpoints from remote", r.endpoint.len());
                    for endpoint in r.endpoint {
                        response = response.with_endpoint(endpoint);
                    }
                }
                Ok(Err(e)) => error!("Failed to get_flight_info from remote with error {e}"),
                Err(e) => {
                    error!("Failed to get_flight_info from remote with error {e}")
                }
            }
        }
        Ok(response)
    }

    /// Processes the first flightdata message in a do_put request, which is expected
    /// to contain metadata needed to identify the request the incoming data is in
    /// response to.
    async fn process_first_do_put_flightdata(
        &self,
        first_data: Result<FlightData, Status>,
        db: &mut PgDb<'_>,
    ) -> Result<(Uuid, Uuid, Arc<Schema>), Status> {
        // These values should be initialized in the first batch of FlightData, otherwise error is thrown
        let remote_task_id;
        let local_task_id;

        let schema = match first_data {
            Ok(data) => {
                match &data.flight_descriptor {
                    Some(desc) => {
                        if desc.path.len() != 2 {
                            return Err(Status::invalid_argument(
                                "do_put expects first batch flight_descriptor.path to contain exactly two strings, \
                            representing the task uuid from the remote relay and the corresponding local task uuid respectively."));
                        }
                        let desc_remote_task_id = desc.path[0].clone();
                        let desc_local_task_id = desc.path[1].clone();
                        let _remote_task = db
                            .get_remote_query_task(
                                uuid::Uuid::parse_str(&desc_local_task_id).map_err(|_e| {
                                    Status::invalid_argument(format!(
                                        "Could not parse task_id as uuid: {desc_local_task_id}"
                                    ))
                                })?,
                            )
                            .await
                            .map_err(|_e| {
                                Status::internal(format!(
                                    "Did not find remote task id {desc_local_task_id}"
                                ))
                            })?;
                        remote_task_id = Uuid::parse_str(&desc_remote_task_id).map_err(|_e| {
                            Status::internal("unable to parse desc_remote_task_id as uuid")
                        })?;
                        local_task_id = Uuid::parse_str(&desc_local_task_id).map_err(|_e| {
                            Status::internal("unable to parse desc_origin_task_id as uuid")
                        })?;
                    }
                    None => {
                        return Err(Status::invalid_argument(
                            "First batch of data missing valid task_id!",
                        ))
                    }
                };

                Arc::new(
                    try_schema_from_flatbuffer_bytes(&data.data_header).map_err(|_e| {
                        Status::invalid_argument(
                            "Could not deserialize schema in first do_put message!",
                        )
                    })?,
                )
            }
            Err(e) => return Err(e.to_owned()),
        };
        // These values should be initialized in the first batch of FlightData, otherwise error is thrown
        Ok((remote_task_id, local_task_id, schema))
    }
}

#[tonic::async_trait]
impl FlightService for FlightRelay {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Retrieves an asynchronously generated query result (initiated via a REST call to /query)
    /// or a synchronously generated query result (initiated via get_flight_info).
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let (fingerprint, subject_dn, issuer_dn) =
            extract_certs(&request, &self.client_cert_header)?;

        info!(
            "Got do_get request from: subject: {}, issuer: {}, fingerprint: {}",
            subject_dn, issuer_dn, fingerprint
        );

        let mut db = PgDb::try_from_pool(&self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("failed to connect to database! {e}")))?;

        let flight_info_ticket: FlightInfoTicket =
            serde_json::from_slice(&request.into_inner().ticket)
                .map_err(|_| Status::invalid_argument("Passed Ticket is not valid!"))?;

        let task_id = flight_info_ticket.task_id;

        debug!("Request is for task {task_id}");

        let retreiving_user = db
            .get_user_by_x509_fingerprint(&fingerprint)
            .await
            .map_err(|_| Status::permission_denied("unrecognized user"))?;

        let (con, source, task, request, _relay) =
            db.get_query_task(task_id).await.map_err(|e| {
                Status::invalid_argument(format!("No query exists with id {task_id}, error {e}"))
            })?;

        // Access denied and no query exists intentionally give same response to prevent
        // brute forcing valid Uuids.
        match request.origin_info.origin_user {
            Some(origin_user) => {
                if origin_user.x509_sha256 != retreiving_user.x509_sha256 {
                    warn!("Rejecting request for valid Uuid to user with fingerprint {fingerprint} which does not match original requester!.");
                    return Err(Status::invalid_argument(format!(
                        "No query exists with id {task_id}"
                    )));
                }
            }
            None => {
                error!("Origin user is not set for query with id {task_id}");
                return Err(Status::invalid_argument(format!(
                    "No query exists with id {task_id}"
                )));
            }
        }

        let rb_stream = self.execute_query_task(con, source, task).await?;

        let flight_data_stream = FlightDataEncoderBuilder::new()
            .build(rb_stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(|e| Status::from_error(Box::new(e)));

        debug!("Sending data stream response...");

        Ok(Response::new(
            Box::pin(flight_data_stream) as Self::DoGetStream
        ))
    }

    /// Handshake is not needed since authentication is handled at the TCP layer as the server
    /// and client verify each other's identities via mTLS.
    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let mut db = PgDb::try_from_pool(&self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("failed to connect to database! {e}")))?;

        let all_information = db
            .get_all_information()
            .await
            .map_err(|e| Status::from_error(Box::new(e)))?;

        let stream = futures::stream::iter(all_information.into_iter().map(|t| {
            let ticket = serde_json::to_vec(&t).map_err(|e| {
                error!("Unexpected error encoding flight_info_ticket as json {e}");
                Status::internal("Unexpected internal error")
            })?;
            Ok::<_, Status>(FlightInfo::new().with_endpoint(
                FlightEndpoint::new().with_ticket(Ticket::new(Into::<Bytes>::into(ticket))),
            ))
        }));
        Ok(Response::new(Box::pin(stream) as Self::ListFlightsStream))
    }

    /// This call is invoked initially by a User via a flight client. It will synchronously
    /// propagate get_flight_info calls throughout the network and return to the caller
    /// a list of all endpoints which have relevant data to the query. The caller can then
    /// invoke do_get to retrieve all of the data  from each relay in the network directly.
    /// This method is a synchronous gRPC analog to the rest_server's /query endpoint.
    async fn get_flight_info(
        &self,
        get_info_request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let (fingerprint, subject_dn, issuer_dn) =
            extract_certs(&get_info_request, &self.client_cert_header)?;

        info!(
            "Got get_flight_info request from: subject: {}, issuer: {}, fingerprint: {}",
            subject_dn, issuer_dn, fingerprint
        );

        let mut db = PgDb::try_from_pool(&self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("failed to connect to database! {e}")))?;

        let flight_descriptor = get_info_request.into_inner();
        let query: RawQueryRequest =
            serde_json::from_slice(&flight_descriptor.cmd).map_err(|_e| {
                Status::invalid_argument(
                    "FlightDescriptior.cmd is not a valid JSON encoded RawQueryRequest",
                )
            })?;

        debug!("Got RawQueryRequest: {:?}", query);

        let (direct_requester, requesting_user, originating_relay) =
            verify_query_origination_information(
                &query,
                &mut db,
                fingerprint,
                subject_dn,
                issuer_dn,
                &self.local_fingerprint,
            )
            .await
            .map_err(|e| {
                Status::invalid_argument(format!(
                    "Unable to parse origination info with error: {e}"
                ))
            })?;

        // It is possible that two requests bypass this check around the same time. This is OK as the database will later
        // raise a Unique contraint violation error. This check is only for efficiency, the database will always ensure correctness.
        match &query.request_uuid {
            Some(id) => match db.check_if_request_already_received(id).await {
                Ok(_) => {
                    info!("Request id {id} already processed! Returning succesful empty response with no further action taken.");
                    let empty_info = FlightInfo::new();
                    return Ok(Response::new(empty_info));
                }
                Err(e) => debug!("Did not find already existing request with error: {e}"),
            },
            None => (),
        }

        debug!("Checking if sql is allowed and logically valid...");
        let (entity_name, statement) = validate_sql_and_logical_round_trip(&query.sql, &mut db)
            .await
            .map_err(|e| {
                Status::invalid_argument(format!("Query validation failed with error {e}"))
            })?;

        debug!("Post round trip sql: {statement}");

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
                empty response with no further action taken.",
                    q.originator_request_id
                );
                let empty_info = FlightInfo::new();
                return Ok(Response::new(empty_info));
            }
            Err(e) => return Err(Status::internal(e.to_string())),
        };

        debug!("Mapping QueryRequest to local queries");
        let created_tasks = map_and_create_local_tasks(
            &statement,
            &query,
            &entity_name,
            &request,
            &mut db,
            &direct_requester,
            &requesting_user,
        )
        .await
        .map_err(|e| {
            error!("{e}");
            Status::internal("Unexpected internal error")
        })?;

        let mut response = self
            .create_flight_info_response(flight_descriptor, &mut db, created_tasks)
            .await?;

        response = self
            .update_flight_info_response_from_remotes(
                response,
                &mut db,
                &query,
                &statement,
                &entity_name,
                &request,
                originating_relay,
                requesting_user,
            )
            .await?;

        debug!("Sending response: {response:?}");

        Ok(Response::new(response))
    }

    /// This call is invoked by other Relay's QueryRunner service to asynchronously send
    /// the result of a query propagated via the rest API. The do_put call stores the
    /// data as a parquet file in an ObjectStore and stores metadata in the database
    /// so that the rest API can retrieve this remotely generated result on demand.
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let (fingerprint, subject_dn, issuer_dn) =
            extract_certs(&request, &self.client_cert_header)?;
        info!(
            "Got do_put request from: subject: {}, issuer: {}, fingerprint: {}",
            subject_dn, issuer_dn, fingerprint
        );

        let mut flight_stream = request.into_inner();
        let mut db = PgDb::try_from_pool(&self.db_pool)
            .await
            .map_err(|e| Status::internal(format!("failed to connect to database! {e}")))?;

        let (remote_task_id, local_task_id, schema) =
            if let Some(first_data) = flight_stream.message().await.transpose() {
                self.process_first_do_put_flightdata(first_data, &mut db)
                    .await?
            } else {
                return Err(Status::internal("missing first flight data message!"));
            };
        let mut new_flight = NewFlightStream {
            query_task_remote_id: local_task_id,
            remote_fingerprint: fingerprint,
            flight_id: remote_task_id,
            status: FlightStreamStatus::Started,
        };

        db.upsert_flight_stream(&new_flight)
            .await
            .map_err(|e| Status::internal(format!("Failed to upsert new flight! Error: {}", e)))?;

        let schema_clone = schema.clone();
        let dictionaries_by_id = Arc::new(HashMap::new());

        // ResultManager currently sending arrow schema twice. This throws the second schema message away...
        // TODO: don't send the schema twice.
        flight_stream.message().await?;

        let rb_stream = Box::pin(
            flight_stream
                .map(move |data| match data {
                    Ok(data) => Ok((data, schema_clone.clone(), dictionaries_by_id.clone())),
                    Err(e) => Err(e),
                })
                .and_then(|(data, inner_schema, id_dict)| async move {
                    flight_data_to_arrow_batch(&data, inner_schema, &id_dict)
                        .map_err(|e| Status::internal(e.to_string()))
                })
                .map_err(|e| DataFusionError::Execution(e.to_string())),
        );

        self.result_manager
            .write_task_result(&remote_task_id, rb_stream, schema.clone())
            .await
            .map_err(|e| Status::internal(format!("Writing stream failed with err {e}")))?;

        new_flight.status = FlightStreamStatus::Complete;
        db.upsert_flight_stream(&new_flight).await.map_err(|e| {
            Status::internal(format!("Failed to update flight status! Error: {}", e))
        })?;
        let do_put_result = PutResult {
            app_metadata: "success".into(),
        };
        Ok(Response::new(
            Box::pin(futures::stream::once(async { Ok(do_put_result) })) as Self::DoPutStream,
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}
