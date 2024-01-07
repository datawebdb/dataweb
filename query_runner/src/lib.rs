use std::env;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use datafusion::physical_plan::SendableRecordBatchStream;
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::AsyncPgConnection;
use mesh::conf::EnvConfigSettings;
use mesh::crud::PgDb;
use mesh::error::MeshError;
use mesh::execute::data_stores::try_connect;
use mesh::execute::result_manager::ResultManager;
use mesh::messaging::{
    initialize_consumer, GenericMessage, MessageBrokerOptions, MessageConsumer, QueryTaskMessage,
};
use mesh::model::data_stores::options::file_directory::FileDirectorySource;
use mesh::model::data_stores::options::SourceFileType;
use mesh::model::data_stores::{DataConnection, DataSource};
use mesh::model::query::{Query, QueryOriginationInfo, QueryTaskRemoteStatus, QueryTaskStatus};
use reqwest::Client;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug)]
pub enum ExecutionError {
    InvalidMessage((u64, String)),
    ConnectionError(MeshError),
    QueryFailed((u64, Uuid, MeshError)),
}

pub type Result<T, E = ExecutionError> = std::result::Result<T, E>;

async fn execute_query(
    con: DataConnection,
    source: DataSource,
    query: Query,
) -> std::result::Result<SendableRecordBatchStream, MeshError> {
    let mut runner = try_connect(con, source).await?;
    runner.execute_stream(query).await
}
struct MessageProcessor<'a> {
    db: PgDb<'a>,
    consumer: Box<dyn MessageConsumer>,
    result_manager: Arc<ResultManager>,
    reqw_client: Client,
}

impl<'a> MessageProcessor<'a> {
    async fn init(
        env_conf: &EnvConfigSettings,
        pool: &'a Pool<AsyncPgConnection>,
        in_memory_msg_opts: &Option<MessageBrokerOptions>,
    ) -> MessageProcessor<'a> {
        let message_options = match in_memory_msg_opts {
            Some(opts) => opts.clone(),
            None => env_conf.msg_broker_opts.clone(),
        };
        let consumer = initialize_consumer(&message_options)
            .await
            .expect("failed to create message consumer");

        let result_source = FileDirectorySource {
            bucket: env_conf.result_bucket.clone(),
            region: env_conf.result_region.clone(),
            prefix: env_conf.result_prefix.clone(),
            file_type: SourceFileType::Parquet,
        };

        let result_manager = Arc::new(
            ResultManager::try_initialize(
                env_conf.result_object_store.clone(),
                result_source,
                env_conf
                    .read_client_cert()
                    .expect("Could not read client cert"),
                env_conf
                    .read_client_key()
                    .expect("Could not read client key"),
                env_conf
                    .read_client_cacert_pem()
                    .expect("Could not read cacert"),
            )
            .expect("Failed to initialize result manager!"),
        );

        let db = PgDb::try_from_pool(pool)
            .await
            .expect("unable to get connection from pool!");

        let iden_pem = env_conf
            .read_client_identity_pem()
            .expect("Could not read client iden pem");
        let pkcs8 =
            reqwest::Identity::from_pem(&iden_pem).expect("could not parse client cert and key");

        let mut client = reqwest::Client::builder().use_rustls_tls().identity(pkcs8);

        let cert = reqwest::Certificate::from_pem(
            &env_conf
                .read_client_cacert_pem()
                .expect("Could not open cacert"),
        )
        .expect("Could not parse cacert");
        client = client.add_root_certificate(cert);
        let reqw_client = client.build().expect("client build err");

        Self {
            db,
            consumer,
            result_manager,
            reqw_client,
        }
    }

    async fn process_local_query_task(
        &mut self,
        msg_id: u64,
        task_message: QueryTaskMessage,
    ) -> Result<()> {
        let (con, source, task, request, _relay) = self
            .db
            .get_query_task(task_message.id)
            .await
            .map_err(|e| ExecutionError::InvalidMessage((msg_id, e.to_string())))?;
        // TODO: implement timeout mechanism in case a query runner dies while holding a task as "in progress"
        if matches!(task.status, QueryTaskStatus::Queued) {
            self.db
                .update_task_status(task.id, QueryTaskStatus::InProgress)
                .await
                .map_err(ExecutionError::ConnectionError)?;
            let query = task.task;
            let rb_stream = execute_query(con, source, query)
                .await
                .map_err(|e| ExecutionError::QueryFailed((msg_id, task.id, e)))?;
            let schema = rb_stream.schema();
            match request.origin_info {
                QueryOriginationInfo {
                    origin_relay: None,
                    origin_task_id: None,
                    ..
                } => {
                    self.result_manager
                        .write_task_result(&task.id, rb_stream, schema)
                        .await
                        .map_err(ExecutionError::ConnectionError)?;
                }
                QueryOriginationInfo {
                    origin_relay: Some(originating_relay),
                    origin_task_id: Some(originating_task_id),
                    ..
                } => {
                    self.result_manager
                        .send_result_flight(
                            &task_message.id,
                            &originating_task_id,
                            rb_stream,
                            schema,
                            originating_relay,
                        )
                        .await
                        .map_err(ExecutionError::ConnectionError)?;
                }
                _ => {
                    return Err(ExecutionError::InvalidMessage((
                        msg_id,
                        "Only one of origin_relay \
                or origin_task_id was set. Either both or neither should be set!"
                            .to_string(),
                    )))
                }
            }

            self.db
                .update_task_status(task.id, QueryTaskStatus::Complete)
                .await
                .map_err(ExecutionError::ConnectionError)?;
        }

        Ok(())
    }

    async fn process_remote_query_task(
        &mut self,
        msg_id: u64,
        task_message: QueryTaskMessage,
    ) -> Result<()> {
        let (remote_task, relay) = self
            .db
            .get_remote_query_task(task_message.id)
            .await
            .map_err(|e| ExecutionError::InvalidMessage((msg_id, e.to_string())))?;

        info!(
            "Sending {:?} to {:?}",
            remote_task.task.originating_task_id, relay
        );
        let task_request = remote_task.task;

        let r = self
            .reqw_client
            .post(format!("{}/query", relay.rest_endpoint))
            .json(&task_request)
            .send()
            .await
            .map_err(|e| ExecutionError::ConnectionError(MeshError::RemoteError(e.to_string())))?;

        match r.text().await {
            Ok(s) => info!("Response from remote: {s}"),
            Err(e) => {
                error!("Failed to parse response as text with e {e}");
            }
        }

        self.db
            .update_remote_task_status(task_message.id, QueryTaskRemoteStatus::Submitted)
            .await
            .map_err(ExecutionError::ConnectionError)?;
        Ok(())
    }

    async fn process_message(&mut self) -> Result<()> {
        info!("Awaiting messages...");
        let (msg_id, msg) = self.consumer.receive_message().await.map_err(|e| match e {
            MeshError::BadMessage((id, s)) => ExecutionError::InvalidMessage((id, s)),
            _ => ExecutionError::ConnectionError(e),
        })?;
        match msg {
            GenericMessage::LocalQueryTask(task_message) => {
                self.process_local_query_task(msg_id, task_message).await?
            }
            GenericMessage::RemoteQueryTask(task_message) => {
                self.process_remote_query_task(msg_id, task_message).await?
            }
        };

        self.consumer
            .ack_message(msg_id)
            .await
            .map_err(ExecutionError::ConnectionError)?;

        Ok(())
    }
}

/// Runs a single async task which consumes and processes messages from the queue one by one.
/// Each worker may spawn many parallel tasks to execute each individual message, especially
/// in the case of using in process DataFusion as the execution engine.
async fn run_worker(in_memory_msg_opts: Option<MessageBrokerOptions>) -> Result<()> {
    let env_conf = EnvConfigSettings::init();
    let config =
        AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(&env_conf.db_url);
    let pool = Pool::builder()
        .max_size(1)
        .build(config)
        .await
        .expect("pool failed to start");

    let mut processor = MessageProcessor::init(&env_conf, &pool, &in_memory_msg_opts).await;

    let mut connection_err_count = 0;
    let max_connection_err_count = 5;
    loop {
        match processor.process_message().await {
            Ok(_) => {
                connection_err_count = 0;
            }
            Err(e) => match e {
                ExecutionError::ConnectionError(e) => {
                    error!("Got connection error {e}");
                    connection_err_count += 1;
                    if connection_err_count > max_connection_err_count {
                        panic!("Got more than {max_connection_err_count} connection errors! query_runner shutting down...")
                    }
                    tokio::time::sleep(Duration::from_secs(2 ^ connection_err_count)).await;
                }
                ExecutionError::QueryFailed((msg_id, task_id, e)) => {
                    match processor.db.update_task_status(task_id, QueryTaskStatus::Failed).await {
                            Ok(()) => error!("Query task {task_id} failed with error: {e}!"),
                            Err(e2) => error!("Query task {task_id} failed with error: {e}! Failed to mark query as failed with err: {e2}!")
                        }
                    match processor.consumer.ack_message(msg_id).await {
                        Ok(()) => (),
                        Err(e) => error!("Failed query message failed to delete with error {e}!"),
                    }
                }
                ExecutionError::InvalidMessage((msg_id, e)) => {
                    error!("Message id: {msg_id} is invalid with error {e}, deleting message!");
                    match processor.consumer.ack_message(msg_id).await {
                        Ok(()) => (),
                        Err(e) => error!("invalid message failed to delete with error {e}!"),
                    }
                }
            },
        }
    }
}

pub async fn run(in_memory_msg_opts: Option<MessageBrokerOptions>) -> Result<()> {
    // By default we run 1 async task per std::thread::available_parallelism. A fewer number of tasks
    // may be optimal if memory is low or if each individual query spawns many async tasks itself.
    let min_parallelism_per_query_worker =
        env::var("MIN_PARALLELISM_PER_QUERY_WORKER").unwrap_or("1".to_string());
    let min_parallelism_per_query_worker: usize = min_parallelism_per_query_worker
        .parse()
        .unwrap_or_else(|_| panic!("Unable to parse {min_parallelism_per_query_worker} as usize!"));
    let available_parallelism: usize = std::thread::available_parallelism()
        .unwrap_or(NonZeroUsize::try_from(1).unwrap())
        .into();

    let num_workers = std::cmp::max(available_parallelism / min_parallelism_per_query_worker, 1);
    info!("Got {min_parallelism_per_query_worker} min_parallelism_per_query_worker and {available_parallelism} available_parallelism");
    info!("Starting {num_workers} query_runner tasks!");

    let mut taskset: tokio::task::JoinSet<Result<()>> = tokio::task::JoinSet::new();
    for _ in 0..num_workers {
        let in_memory_msg_opts_clone = in_memory_msg_opts.clone();
        taskset.spawn(async move { run_worker(in_memory_msg_opts_clone).await });
    }

    // All tasks should run forever, so we panic if any in fact exit.
    match taskset.join_next().await {
        Some(Ok(Ok(_))) => panic!("QueryRunner worker shut down with no error!"),
        Some(Ok(Err(e))) => panic!("QueryRunner worker shut down with error: {e:?}"),
        Some(Err(e)) => panic!("QueryRunner worker join_error {e}"),
        None => unreachable!("The taskset should never be empty!"),
    }
}
