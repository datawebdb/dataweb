use std::sync::Arc;

use actix_web::HttpResponse;
#[allow(deprecated)]
use arrow::json::writer::record_batches_to_json_rows;
use arrow::record_batch::RecordBatch;

use tracing::{error, warn};

use crate::error::{RelayError, Result};

use mesh::crud::PgDb;
use mesh::execute::result_manager::ResultManager;

use mesh::model::query::{
    FlightStream, FlightStreamStatus, QueryTask, QueryTaskRemote, QueryTaskStatus,
};

use datafusion::common::DataFusionError;
use futures::TryStreamExt;

use serde_json::Value;

/// Counts how many local and remote tasks in total are complete, failed, or in progress
pub(crate) fn count_task_status(
    tasks: &[QueryTask],
    remote_flight: &[(QueryTaskRemote, FlightStream)],
) -> (usize, usize, usize) {
    let mut complete = 0;
    let mut failed = 0;
    let mut in_progress = 0;

    for task in tasks.iter() {
        match task.status {
            QueryTaskStatus::Complete => complete += 1,
            QueryTaskStatus::Failed => failed += 1,
            QueryTaskStatus::Queued | QueryTaskStatus::InProgress => in_progress += 1,
        }
    }

    for (remote, flight) in remote_flight.iter() {
        match flight.status {
            FlightStreamStatus::Complete => complete += 1,
            FlightStreamStatus::Failed => failed += 1,
            FlightStreamStatus::Started => in_progress += 1,
            FlightStreamStatus::Invalid => warn!(
                "Flight stream {} for remote request {} is logged as invalid.",
                flight.id, remote.id
            ),
        }
    }
    (complete, failed, in_progress)
}

/// Converts a [RecordBatch] to a serialized NDJSON object, injecting additional metadata into the JSON records prior to
/// serializaiton.
pub(crate) fn convert_rb_to_serialized_json_records(
    batch: RecordBatch,
    metadata: Arc<Value>,
) -> Result<bytes::Bytes, DataFusionError> {
    #[allow(deprecated)]
    let js = record_batches_to_json_rows(&[&batch])
        .map_err(|_e| DataFusionError::Execution("Serialization to json failed".into()))?;
    let mut serialized = vec![];
    let metadata: Arc<Value> = metadata;
    for mut val in js {
        // Should be possible to avoid the clone here somehow
        let metadata_clone: Value = metadata.as_ref().to_owned();
        val.insert("_relay_metadata_".to_string(), metadata_clone);
        serialized.extend(
            serde_json::to_vec(&val)
                .map_err(|_e| DataFusionError::Execution("Serialization to json failed".into()))?,
        );
        serialized.extend_from_slice(b"\n");
    }
    Ok(bytes::Bytes::from(serialized))
}

/// Creates a HttpResponse::Ok().streaming(...) where the returned stream is all of the local and remote
/// task results interleaved with additional injected metadata, serialized as NDJSON records.
pub(crate) async fn stream_all_task_results(
    db: &mut PgDb<'_>,
    local_fingerprint: &Arc<String>,
    result_manager: &Arc<ResultManager>,
    tasks: Vec<QueryTask>,
    flights: Vec<(QueryTaskRemote, FlightStream)>,
) -> Result<HttpResponse> {
    let rb_stream_converter =
        |(batch, metadata)| async move { convert_rb_to_serialized_json_records(batch, metadata) };
    let mut all_streams = Vec::with_capacity(tasks.len() + flights.len());

    let local_relay = db.get_relay_by_x509_fingerprint(local_fingerprint).await?;
    for task in tasks {
        if matches!(task.status, QueryTaskStatus::Complete) {
            let data_source_id = task.data_source_id;
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "_source_relay_".to_string(),
                serde_json::to_value(local_relay.id).map_err(|_e| {
                    error!("Failed to serde_json {:?}", local_relay.id);
                    RelayError {
                        msg: "Failed to serialize relay object".to_string(),
                    }
                })?,
            );

            metadata.insert(
                "_source_id_".to_string(),
                serde_json::to_value(data_source_id).map_err(|_e| {
                    error!("Failed to serde_json {data_source_id:?}");
                    RelayError {
                        msg: "Failed to serialize relay object".to_string(),
                    }
                })?,
            );

            let metadata_arc = Arc::new(serde_json::Value::Object(metadata));
            let inject_closure: Box<dyn Fn(RecordBatch) -> (RecordBatch, Arc<Value>)> =
                Box::new(move |b| (b, metadata_arc.clone()));
            all_streams.push(Box::pin(
                result_manager
                    .get_task_result(task.id)
                    .await?
                    .map_ok(inject_closure)
                    .and_then(rb_stream_converter),
            ));
        }
    }

    for (_remote_task, flight) in flights {
        if matches!(flight.status, FlightStreamStatus::Complete) {
            let data_source_id = flight.flight_id;
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "_source_relay_".to_string(),
                serde_json::to_value(&flight.remote_fingerprint).map_err(|_e| {
                    error!("Failed to serde_json {:?}", flight.remote_fingerprint);
                    RelayError {
                        msg: "Failed to serialize relay object".to_string(),
                    }
                })?,
            );

            metadata.insert(
                "_source_id_".to_string(),
                serde_json::to_value(data_source_id).map_err(|_e| {
                    error!("Failed to serde_json {data_source_id:?}");
                    RelayError {
                        msg: "Failed to serialize relay object".to_string(),
                    }
                })?,
            );

            let metadata_arc = Arc::new(serde_json::Value::Object(metadata));
            let inject_closure: Box<dyn Fn(RecordBatch) -> (RecordBatch, Arc<Value>)> =
                Box::new(move |b| (b, metadata_arc.clone()));
            all_streams.push(Box::pin(
                result_manager
                    .get_task_result(flight.flight_id)
                    .await?
                    .map_ok(inject_closure)
                    .and_then(rb_stream_converter),
            ));
        }
    }

    let merged_stream = futures::stream::select_all(all_streams);
    Ok(HttpResponse::Ok().streaming(merged_stream))
}
