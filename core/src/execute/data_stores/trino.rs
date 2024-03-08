use std::env;
use std::pin::Pin;
use std::sync::Arc;

use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_json::ReaderBuilder;

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{EmptyRecordBatchStream, SendableRecordBatchStream};
use futures::StreamExt;
use prusto::auth::Auth;
use prusto::{Client, ClientBuilder, DataSet, Row};
use tracing::debug;

use crate::error::{MeshError, Result};
use crate::model::data_stores::options::trino::{TrinoConnection, TrinoSource};
use crate::model::query::Query;

use super::QueryRunner;

/// Provides [QueryRunner] impl leveraging an external Trino cluster
/// as the execution engine.
pub struct TrinoRunner {
    pub client: Arc<Client>,
}

impl TryFrom<(TrinoConnection, TrinoSource)> for TrinoRunner {
    type Error = MeshError;

    fn try_from(value: (TrinoConnection, TrinoSource)) -> Result<Self> {
        let (con, _source) = value;
        let auth = if con.password.is_empty() {
            Auth::Basic(con.user.clone(), None)
        } else {
            let pass = env::var(&con.password).map_err(|_e| {
                MeshError::Internal(format!(
                    "Expected trino password to be set in {} \
                env variable, but it is unset!",
                    con.password
                ))
            })?;
            Auth::Basic(con.user.clone(), Some(pass))
        };
        let client = if con.secure {
            ClientBuilder::new(&con.user, &con.host)
                .port(con.port.parse().map_err(|_e| {
                    MeshError::SerDe(format!("Cannot parse {} as a port", con.port))
                })?)
                .auth(auth)
                .secure(con.secure)
                .build()
                .map_err(|e| {
                    MeshError::RemoteError(format!("Failed to connect to trino cluster: {e}"))
                })?
        } else {
            ClientBuilder::new(&con.user, &con.host)
                .port(con.port.parse().map_err(|_e| {
                    MeshError::SerDe(format!("Cannot parse {} as a port", con.port))
                })?)
                .build()
                .map_err(|e| {
                    MeshError::RemoteError(format!("Failed to connect to trino cluster: {e}"))
                })?
        };

        Ok(Self {
            client: Arc::new(client),
        })
    }
}

fn trino_dataset_to_ndjson(dset: DataSet<Row>, schema: SchemaRef) -> Vec<serde_json::Value> {
    let (_, rows) = dset.split();
    let mut json_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut row_map = serde_json::Map::new();
        for (js_val, field) in row.into_json().into_iter().zip(schema.fields().iter()) {
            row_map.insert(field.name().to_string(), js_val);
        }
        json_rows.push(serde_json::Value::Object(row_map))
    }
    json_rows
}

fn infer_arrow_schema_from_dataset(dset: &DataSet<Row>) -> Result<SchemaRef> {
    // To avoid this clone, will need to modify prusto to allow getting a references to PrestoTy
    let dset = dset.clone();
    let (trino_types, mut rows) = dset.split();
    let first_row = rows.swap_remove(0);
    let mut row_map = serde_json::Map::new();
    for (js_val, trino_type) in first_row.into_json().into_iter().zip(trino_types.iter()) {
        row_map.insert(trino_type.0.clone(), js_val);
    }
    let jsobj = serde_json::Value::Object(row_map);
    let schema = Arc::new(infer_json_schema_from_iterator(std::iter::once(Ok(jsobj)))?);
    Ok(schema)
}

async fn execute_stream(client: Arc<Client>, query: Query) -> Result<SendableRecordBatchStream> {
    let client_clone = client.clone();
    let sql = Some(query.sql.clone());
    let sql_check = query.sql;
    let trino_stream = futures::stream::try_unfold(
        (sql, sql_check, client_clone),
        |(get_next, sql_check, client_clone)| async move {
            if let Some(next) = get_next {
                let res = if next == sql_check {
                    client_clone
                        .get::<Row>(next)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("{e}")))?
                } else {
                    client_clone
                        .get_next::<Row>(&next)
                        .await
                        .map_err(|e| DataFusionError::Execution(format!("{e}")))?
                };
                Ok(Some((
                    res.data_set,
                    (res.next_uri, sql_check, client_clone),
                )))
            } else {
                Ok(None)
            }
        },
    )
    .filter_map(|data| async {
        match data {
            Ok(d) => match d {
                Some(dset) => {
                    if dset.as_slice().is_empty() {
                        None
                    } else {
                        Some(Ok(dset))
                    }
                }
                None => None,
            },
            Err(e) => Some(Err(e)),
        }
    });

    // Peek at the first batch of data from trino to impute the appropraite arrow schema
    // for the returned data. Otherwise return an EmptyRecordBatchStream if there is no data.
    let mut peekable = Box::pin(trino_stream).peekable();
    let schema = if let Some(data) = Pin::new(&mut peekable).peek().await {
        match data {
            Ok(d) => match query.return_schema {
                Some(schema) => {
                    debug!("Trino runner attempting to serialize data with arrow schema {schema}");
                    Arc::new(schema)
                }
                None => {
                    debug!("No schema specified for trino runner. Attempting to infer...");
                    let schema = infer_arrow_schema_from_dataset(d)?;
                    debug!("Inferred {schema}");
                    schema
                }
            },
            Err(e) => return Err(MeshError::RemoteError(format!("{e}"))),
        }
    } else {
        return match query.return_schema {
            Some(schema) => Ok(Box::pin(EmptyRecordBatchStream::new(Arc::new(schema)))),
            None => Ok(Box::pin(EmptyRecordBatchStream::new(Arc::new(
                Schema::empty(),
            )))),
        };
    };

    // Map the Stream<Item=DataSet<Row>> json data from trino into
    // a SendableRecordBatchStream with the schema imputed from the first batch
    let schema_clone = schema.clone();
    let rb_stream = peekable.map(move |data| match data {
        Ok(d) => {
            debug!("trino raw row: {:?}", &d.as_slice()[..5]);
            let ndjson = trino_dataset_to_ndjson(d, schema_clone.clone());
            debug!("trino json value row: {:?}", &ndjson[..5]);
            let mut decoder = ReaderBuilder::new(schema_clone.clone()).build_decoder()?;
            decoder.serialize(&ndjson)?;
            let rb = decoder.flush()?.ok_or(DataFusionError::Execution(
                "Got empty batch from trino!".to_string(),
            ))?;
            debug!("trino recordbatch: {:?}",rb.slice(0, 5));
            Ok(rb)
        }
        Err(e) => Err(e),
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, rb_stream)))
}

#[async_trait]
impl QueryRunner for TrinoRunner {
    async fn execute_stream(&mut self, query: Query) -> Result<SendableRecordBatchStream> {
        // Note: async_trait macro causes a higher ranked lifetime error if the
        // execute_stream helper function is included literally in this method.
        Ok(execute_stream(self.client.clone(), query).await?)
    }
}
