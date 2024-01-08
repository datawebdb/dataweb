use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_schema::{Field, SchemaBuilder};
use tracing::debug;

use crate::{utils::get_flight_client, web_source::DataWebEntity};
use bytes::Bytes;
use datafusion::{
    common::Result, datasource::TableProvider, error::DataFusionError,
    execution::context::SessionContext,
};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
pub struct Information {
    pub id: Uuid,
    pub name: String,
    pub arrow_dtype: ArrowDataType,
    pub entity_id: Uuid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ArrowDataType {
    pub inner: DataType,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum InfoType {
    Identifier,
    Attribute,
}

pub async fn register_web_sources(
    ctx: &SessionContext,
    local_relay_endpoint: Arc<String>,
    client_cert: Arc<Vec<u8>>,
    client_key: Arc<Vec<u8>>,
    ca_cert: Arc<Vec<u8>>,
) -> Result<Vec<Arc<dyn TableProvider>>> {
    // 1. Connect to local_relay
    let mut client = get_flight_client(
        client_cert.clone(),
        client_key.clone(),
        ca_cert.clone(),
        local_relay_endpoint.as_ref(),
    )
    .await?;

    // 2. list_flights to get all Entities
    let mut result = client
        .list_flights(Bytes::new())
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // 3. Register a provider for each Entity
    let mut providers = vec![];
    while let Some(mut flight_info) = result
        .next()
        .await
        .transpose()
        .map_err(|e| DataFusionError::External(Box::new(e)))?
    {
        let (entity, information) = serde_json::from_slice::<(String, Vec<Information>)>(
            &flight_info.endpoint.swap_remove(0).ticket.unwrap().ticket,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

        debug!("Registering: entity {entity}, information: {information:?}");

        let mut schema_builder = SchemaBuilder::new();
        for info in information {
            schema_builder.push(Field::new(info.name, info.arrow_dtype.inner, true));
        }
        let schema = Arc::new(schema_builder.finish());
        let entity_provider: Arc<dyn TableProvider> = Arc::new(DataWebEntity {
            entity_name: entity.clone(),
            schema,
            local_relay_endpoint: local_relay_endpoint.clone(),
            client_cert: client_cert.clone(),
            client_key: client_key.clone(),
            ca_cert: ca_cert.clone(),
        });
        ctx.register_table(&entity, entity_provider.clone())?;
        providers.push(entity_provider);
    }
    Ok(providers)
}
