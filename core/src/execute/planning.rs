use std::{any::Any, sync::Arc};

use arrow_schema::{DataType, SchemaRef};
use datafusion::error::Result;
use datafusion::{
    common::plan_err,
    config::ConfigOptions,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF},
    sql::{planner::ContextProvider, TableReference},
};

/// DataFusion logical planning ContextProvider for a single Entity.
pub struct EntityContext {
    entity: String,
    schema: SchemaRef,
    options: ConfigOptions,
}

impl EntityContext {
    pub(crate) fn new(entity: &str, schema: SchemaRef) -> Self {
        Self {
            entity: entity.to_string(),
            schema,
            options: ConfigOptions::default(),
        }
    }
}

impl ContextProvider for EntityContext {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        if name.table() == self.entity {
            Ok(Arc::new(EntitySource {
                schema: self.schema.clone(),
            }))
        } else {
            plan_err!(
                "Unexpected Entity encountered {name} while planning for entity {}",
                self.entity
            )
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udfs_names(&self) -> Vec<String> {
        vec![]
    }

    fn udafs_names(&self) -> Vec<String> {
        vec![]
    }

    fn udwfs_names(&self) -> Vec<String> {
        vec![]
    }
}

struct EntitySource {
    schema: SchemaRef,
}

impl TableSource for EntitySource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
