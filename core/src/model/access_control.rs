use crate::model::relay::Relay;
use crate::model::user::User;
use diesel::prelude::*;
use diesel::{AsExpression, FromSqlRow};
use diesel_as_jsonb::AsJsonb;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use uuid::Uuid;

use crate::model::data_stores::DataSource;
use crate::schema::{default_source_permission, relay_source_permission, user_source_permission};

// Policy evaluation logic:
// 1. Start with default source permission for given DataSource
// 2. Add any columns and rows explicitly allowed for user/relay involved in query
// 3. Remove any columns and rows explicitly denied for user/relay involved in query

/// Defines the columns and rows any authenticated [User] or [Relay]
/// is permitted to access for a specific [DataSource] in the absense
/// of explicit allows or denys for the [User] or [Relay]
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Identifiable, Associations, Debug, PartialEq,
)]
#[diesel(belongs_to(DataSource))]
#[diesel(table_name = default_source_permission)]
pub struct DefaultSourcePermission {
    pub id: Uuid,
    pub data_source_id: Uuid,
    pub source_permission: SourcePermission,
}

/// Database object for the relay_source_permission table. Holds a [SourcePermission] for specific
/// [Relay]s
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Identifiable, Associations, Debug, PartialEq,
)]
#[diesel(belongs_to(DataSource))]
#[diesel(belongs_to(Relay))]
#[diesel(table_name = relay_source_permission)]
pub struct RelaySourcePermission {
    pub id: Uuid,
    pub data_source_id: Uuid,
    pub relay_id: Uuid,
    pub source_permission: SourcePermission,
}

/// Database object for the user_source_permission table. Holds a [SourcePermission] for specific
/// [User]s
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Identifiable, Associations, Debug, PartialEq,
)]
#[diesel(belongs_to(DataSource))]
#[diesel(belongs_to(User))]
#[diesel(table_name = user_source_permission)]
pub struct UserSourcePermission {
    pub id: Uuid,
    pub data_source_id: Uuid,
    pub user_id: Uuid,
    pub source_permission: SourcePermission,
}

/// Defines the columns and rows of a [DataSource] that a given [Relay]
/// or [User] are permitted to retrieve.
#[derive(Serialize, Deserialize, Debug, AsJsonb, PartialEq)]
pub struct SourcePermission {
    pub columns: ColumnPermission,
    pub rows: RowPermission,
}

impl SourcePermission {
    /// Computes the union of allowed columns and rows between two [ColumnPermission]s.
    pub fn union(&self, other: &SourcePermission) -> SourcePermission {
        let columns = self.columns.union(&other.columns);
        let rows = self.rows.union(&other.rows);
        SourcePermission { columns, rows }
    }

    /// Computes the intersection of allowed columns and rows between two [ColumnPermission]s.
    pub fn intersection(&self, other: &SourcePermission) -> SourcePermission {
        let columns = self.columns.intersection(&other.columns);
        let rows = self.rows.intersection(&other.rows);
        SourcePermission { columns, rows }
    }
}

/// Represents a set of columns for a specific [DataSource]
/// which a given [Relay] or [User] are allowed to access
#[derive(Serialize, Deserialize, Debug, AsJsonb, PartialEq)]
pub struct ColumnPermission {
    pub allowed_columns: HashSet<String>,
}

impl ColumnPermission {
    /// Computes the union of allowed columns between two [ColumnPermission]s.
    pub fn union(&self, other: &ColumnPermission) -> ColumnPermission {
        let allowed_columns: HashSet<_> = self
            .allowed_columns
            .union(&other.allowed_columns)
            .map(|e| e.to_string())
            .collect();

        ColumnPermission { allowed_columns }
    }

    /// Computes the intersection of allowed columns between two [ColumnPermission]s.
    pub fn intersection(&self, other: &ColumnPermission) -> ColumnPermission {
        let allowed_columns: HashSet<_> = self
            .allowed_columns
            .intersection(&other.allowed_columns)
            .map(|e| e.to_string())
            .collect();

        ColumnPermission { allowed_columns }
    }
}

/// Defines filter expressions which restrict the rows which can
/// be accessed
#[derive(Serialize, Deserialize, Debug, AsJsonb, PartialEq)]
pub struct RowPermission {
    /// A sql filter expression which defines the allowed rows:
    /// e.g. "(col1=1 or (col2=2 and name='joe')) and not col3='secret'"
    pub allowed_rows: String,
}

impl RowPermission {
    /// Computes the union of rows bewteen two [RowPermission]s
    /// by combining the two filter expressions with an OR operator
    pub fn union(&self, other: &RowPermission) -> RowPermission {
        RowPermission {
            allowed_rows: format!("({}) OR ({})", self.allowed_rows, other.allowed_rows),
        }
    }

    /// Computes the intersection of rows bewteen two [RowPermission]s
    /// by combining the two filter expressions with an AND operator
    pub fn intersection(&self, other: &RowPermission) -> RowPermission {
        RowPermission {
            allowed_rows: format!("({}) AND ({})", self.allowed_rows, other.allowed_rows),
        }
    }
}
