use arrow_schema::DataType;
use diesel::prelude::*;

use diesel::{prelude::Insertable, AsExpression, FromSqlRow};
use diesel_as_jsonb::AsJsonb;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{entities, information};

/// Represents a name-space for a collection of [Information] which is scoped to an individual
/// [Relay][crate::model::relay::Relay]. Even if two [Relay][crate::model::relay::Relay]s
/// have the same Entity name, they will have distinct [Uuid]s and will be treated
/// as different name-spaces. An Entity can represent any collection of [Information] and is intentionally
/// very flexible. An Entity could represent a complete data model or a real world business entity, such
/// as a customer object, where the contained [Information] pertain to that business entity.
#[derive(Serialize, Deserialize, Queryable, Identifiable, Selectable, Debug, PartialEq)]
#[diesel(table_name = entities)]
pub struct Entity {
    pub id: Uuid,
    pub name: String,
}

/// Represents a distinct unit of information scoped to an individual [Entity] within an individual
/// [Relay][crate::model::relay::Relay].Information can represent anything and it is up to the
/// administrators of a [Relay][crate::model::relay::Relay] to create the approprate
/// [Mapping](crate::model::mappings::Mapping) and [RemoteInfoMapping][crate::model::mappings::RemoteInfoMapping]
/// which allow the [Relay][crate::model::relay::Relay] to convert Information to appropriate SQL queries on
/// local tables and remote [Entity]s.
#[derive(
    Serialize, Deserialize, Queryable, Selectable, Identifiable, Associations, Debug, PartialEq,
)]
#[diesel(belongs_to(Entity))]
#[diesel(table_name = information)]
pub struct Information {
    pub id: Uuid,
    pub name: String,
    pub arrow_dtype: ArrowDataType,
    pub entity_id: Uuid,
}

/// NewType wrapper of [DataType]
#[derive(Debug, PartialEq, Serialize, Deserialize, AsJsonb, Clone)]
pub struct ArrowDataType {
    pub inner: DataType,
}

/// Used when creating a new [Information] object in the database
#[derive(Queryable, Selectable, Insertable, Associations, Debug, PartialEq, AsChangeset)]
#[diesel(belongs_to(Entity))]
#[diesel(table_name = information)]
pub struct NewInformation {
    pub name: String,
    pub arrow_dtype: ArrowDataType,
    pub entity_id: Uuid,
}
