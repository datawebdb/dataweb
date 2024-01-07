use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use datafusion::{
    common::{FileType, GetExt},
    datasource::{
        file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat},
        listing::ListingOptions,
    },
    error::DataFusionError,
    physical_plan::{stream::RecordBatchStreamAdapter, SendableRecordBatchStream},
    prelude::SessionContext,
};

use futures::StreamExt;
use object_store::ObjectStore;
use tracing::debug;
use url::Url;

use crate::{
    error::MeshError,
    model::data_stores::options::{
        file_directory::{FileDirectoryConnection, FileDirectorySource},
        SourceFileType,
    },
};

use crate::error::Result;

use super::{initialize_object_store, Query, QueryRunner};

/// This runner is unqiue in that it uses in process query engine DataFusion to directly query
/// raw files in an [ObjectStore].
pub struct FileDirectoryRunner {
    object_store: Arc<dyn ObjectStore>,
    url: Url,
    file_type: SourceFileType,
    table_name: String,
}

impl TryFrom<(FileDirectoryConnection, FileDirectorySource, String)> for FileDirectoryRunner {
    type Error = MeshError;

    fn try_from(value: (FileDirectoryConnection, FileDirectorySource, String)) -> Result<Self> {
        let (con, source, table_name) = value;
        let object_store = initialize_object_store(con.object_store_type, &source)?;
        Ok(Self {
            object_store,
            url: Url::parse(&con.url)?,
            file_type: source.file_type,
            table_name,
        })
    }
}

#[async_trait]
impl QueryRunner for FileDirectoryRunner {
    async fn execute_stream(&mut self, query: Query) -> Result<SendableRecordBatchStream> {
        let ctx = SessionContext::new();
        ctx.runtime_env()
            .register_object_store(&self.url, self.object_store.clone());

        match self.file_type {
            SourceFileType::CSV => {
                let file_format = CsvFormat::default();
                let listing_options = ListingOptions::new(Arc::new(file_format))
                    .with_file_extension(FileType::CSV.get_ext());
                ctx.register_listing_table(
                    &self.table_name,
                    format!("{}", self.url),
                    listing_options,
                    None,
                    None,
                )
                .await?;
            }
            SourceFileType::JSON => {
                let file_format = JsonFormat::default();
                let listing_options = ListingOptions::new(Arc::new(file_format))
                    .with_file_extension(FileType::JSON.get_ext());
                ctx.register_listing_table(
                    &self.table_name,
                    format!("{}", self.url),
                    listing_options,
                    None,
                    None,
                )
                .await?;
            }
            SourceFileType::Parquet => {
                let file_format = ParquetFormat::default();
                let listing_options = ListingOptions::new(Arc::new(file_format))
                    .with_file_extension(FileType::PARQUET.get_ext());
                ctx.register_listing_table(
                    &self.table_name,
                    format!("{}", self.url),
                    listing_options,
                    None,
                    None,
                )
                .await?;
            }
        };

        debug!("datafusion executing SQL: {}", query.sql);
        let df = ctx.sql(&query.sql).await?;
        // Check if a specific return_schema was specified, and if so
        // attempt to cast the output to the return_schema, otherwise,
        // just return the stream as-is.
        match query.return_schema {
            Some(schema) => {
                let schema: Arc<arrow_schema::Schema> = Arc::new(schema);
                let schema_clone1 = schema.clone();
                let schema_clone2 = schema.clone();
                let stream = df
                    .execute_stream()
                    .await?
                    .map(move |maybe_batch| match maybe_batch {
                        Ok(rb) => rb
                            .columns()
                            .iter()
                            .zip(schema_clone1.fields())
                            .map(|(arr, field)| {
                                arrow::compute::cast(arr, field.data_type())
                                    .map_err(DataFusionError::from)
                            })
                            .collect::<Result<Vec<_>, DataFusionError>>(),
                        Err(e) => Err(e),
                    })
                    .map(move |maybe_converted| match maybe_converted {
                        Ok(arrays) => RecordBatch::try_new(schema_clone2.clone(), arrays)
                            .map_err(DataFusionError::from),
                        Err(e) => Err(e),
                    });
                Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
            }
            None => Ok(df.execute_stream().await?),
        }
    }
}
