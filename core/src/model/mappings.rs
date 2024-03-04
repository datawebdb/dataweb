use super::data_stores::DataField;
use super::entity::Information;

use crate::model::entity::Entity;
use crate::model::relay::Relay;
use crate::schema::{field_mappings, remote_entity_mapping, remote_info_mapping};
use diesel::prelude::*;
use diesel::{prelude::Insertable, AsExpression, FromSqlRow};
use diesel_as_jsonb::AsJsonb;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Ties an individual [Information] to a [DataField] in a specific local
/// [DataSource][crate::model::data_stores::DataSource], and describes
/// how the [DataField] can be converted into the [Information] via a
/// [Transformation].
#[derive(
    Queryable, Selectable, Insertable, Associations, Debug, PartialEq, Serialize, Deserialize,
)]
#[diesel(belongs_to(Information), belongs_to(DataField))]
#[diesel(table_name = field_mappings)]
pub struct Mapping {
    pub information_id: Uuid,
    pub data_field_id: Uuid,
    pub transformation: Transformation,
}

/// Contains a query template which describes how a remote [Entity] can be translated
/// into a local [Entity] via arbitrary SQL logic. The SQL template can be anything
/// which is valid to subtitute in place of a table identifier. E.g. a subquery
/// or even an aggregation or join.
#[derive(
    Queryable,
    Selectable,
    Insertable,
    Associations,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    AsChangeset,
)]
#[diesel(belongs_to(Entity), belongs_to(Relay))]
#[diesel(table_name = remote_entity_mapping)]
pub struct RemoteEntityMapping {
    pub id: Uuid,
    pub sql: String,
    /// The [Relay] which maintains the remote [Entity]
    pub relay_id: Uuid,
    /// The local [Entity] which will be translated
    pub entity_id: Uuid,
    /// The name of the [Entity] on the remote relay being translated
    pub remote_entity_name: String,
}

/// Used to insert a new [RemoteEntityMapping] to the database.
#[derive(
    Queryable,
    Selectable,
    Insertable,
    Associations,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    AsChangeset,
)]
#[diesel(belongs_to(Entity), belongs_to(Relay))]
#[diesel(table_name = remote_entity_mapping)]
pub struct NewRemoteEntityMapping {
    pub sql: String,
    /// The [Relay] which maintains the remote [Entity]
    pub relay_id: Uuid,
    /// The local [Entity] which will be translated
    pub entity_id: Uuid,
    /// The name of the [Entity] on the remote relay being translated
    pub remote_entity_name: String,
}

/// Ties a local [Information] to a logical or literal field of a [RemoteEntityMapping]. This enables
/// the local [Relay] to convert query templates into a name-space that the remote [Relay]
/// understands and can map to its own local [DataField]s. [Transformation] allows specifying
/// additional SQL logic to translate a specific field in the [RemoteEntityMapping] to and from
/// a local [Information].
#[derive(
    Queryable,
    Selectable,
    Insertable,
    Associations,
    Debug,
    PartialEq,
    Serialize,
    Deserialize,
    AsChangeset,
)]
#[diesel(belongs_to(Information), belongs_to(RemoteEntityMapping))]
#[diesel(table_name = remote_info_mapping)]
pub struct RemoteInfoMapping {
    pub remote_entity_mapping_id: Uuid,
    pub information_id: Uuid,
    pub info_mapped_name: String,
    pub transformation: Transformation,
}

/// Contains SQL expression templates which describe how to convert local [Information] to or from local
/// [DataField]s or to or from remote [Information]. The expression must be invertible and both directions
/// specified. As a query propagates the network, a Transformation may be applied and inverted serveral times.
/// If the Transformation is not invertible (in the set theoretic sense, meaning it is a bijective function),
/// then at a minimum some precision will be lost. An example would be casting a float64 to float32: the network
/// cannot guarentee retaining the full float64 precision as a query is transformed/inverted through the
/// propagation process. Depending on the business applicaiton, this loss in precision may be acceptable,
/// but the adminstrators of the relay [Mapping] and [RemoteInfoMapping]s are responsible for ensuring this.
/// A Transformation can only operate on a single [DataField]. For more complex transformations, use a
/// [RemoteEntityMapping] or modify the [DataSource][crate::model::data_stores::DataSource] source_sql field.
#[derive(Debug, PartialEq, Serialize, Deserialize, AsJsonb, Clone)]
pub struct Transformation {
    /// The expression template to transform the other value into the [Information].
    /// Note that "other" could be a local [DataField] or a remote [Information] depending
    /// on the context of this Transformation.
    /// Example: "{v}/10 + 5"
    pub other_to_local_info: String,
    /// The portion of each expression which should be replaced by the DataField path, e.g. "{v}"
    pub replace_from: String,
}

impl Transformation {
    /// Given self as f: X->Y and Y->X and other as g: Y->Z and Z->Y returns a [Transformation]
    /// representing the composition, h=f(g): X->Z and Z->X.
    pub fn compose(&self, other: &Transformation) -> Transformation {
        let other_to_local_info = other
            .other_to_local_info
            .replace(
                &other.replace_from,
                &format!("({})", self.other_to_local_info),
            )
            .replace(&self.replace_from, &other.replace_from);
        Transformation {
            other_to_local_info,
            replace_from: other.replace_from.clone(),
        }
    }
}
