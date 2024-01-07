// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "flight_stream_status"))]
    pub struct FlightStreamStatus;

    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "query_task_remote_status"))]
    pub struct QueryTaskRemoteStatus;

    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "query_task_status"))]
    pub struct QueryTaskStatus;
}

diesel::table! {
    data_connection (id) {
        id -> Uuid,
        name -> Varchar,
        connection_options -> Jsonb,
    }
}

diesel::table! {
    data_field (id) {
        id -> Uuid,
        name -> Varchar,
        data_source_id -> Uuid,
        path -> Varchar,
    }
}

diesel::table! {
    data_source (id) {
        id -> Uuid,
        name -> Varchar,
        source_sql -> Varchar,
        data_connection_id -> Uuid,
        source_options -> Jsonb,
    }
}

diesel::table! {
    default_source_permission (id) {
        id -> Uuid,
        data_source_id -> Uuid,
        source_permission -> Jsonb,
    }
}

diesel::table! {
    entities (id) {
        id -> Uuid,
        name -> Varchar,
    }
}

diesel::table! {
    field_mappings (id) {
        id -> Uuid,
        data_field_id -> Uuid,
        information_id -> Uuid,
        transformation -> Jsonb,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::FlightStreamStatus;

    incoming_flight_streams (id) {
        id -> Uuid,
        query_task_remote_id -> Uuid,
        remote_fingerprint -> Varchar,
        flight_id -> Uuid,
        status -> FlightStreamStatus,
    }
}

diesel::table! {
    information (id) {
        id -> Uuid,
        name -> Varchar,
        arrow_dtype -> Jsonb,
        entity_id -> Uuid,
    }
}

diesel::table! {
    query_request (id) {
        id -> Uuid,
        originator_request_id -> Uuid,
        sql -> Varchar,
        relay_id -> Uuid,
        origin_info -> Jsonb,
        substitution_blocks -> Jsonb,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::QueryTaskStatus;

    query_task (id) {
        id -> Uuid,
        query_request_id -> Uuid,
        data_source_id -> Uuid,
        task -> Jsonb,
        status -> QueryTaskStatus,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::QueryTaskRemoteStatus;

    query_task_remote (id) {
        id -> Uuid,
        query_request_id -> Uuid,
        relay_id -> Uuid,
        task -> Jsonb,
        status -> QueryTaskRemoteStatus,
    }
}

diesel::table! {
    relay_source_permission (id) {
        id -> Uuid,
        data_source_id -> Uuid,
        relay_id -> Uuid,
        source_permission -> Jsonb,
    }
}

diesel::table! {
    relays (id) {
        id -> Uuid,
        name -> Varchar,
        rest_endpoint -> Varchar,
        flight_endpoint -> Varchar,
        x509_sha256 -> Varchar,
        x509_subject -> Varchar,
        x509_issuer -> Varchar,
    }
}

diesel::table! {
    remote_entity_mapping (id) {
        id -> Uuid,
        sql -> Varchar,
        substitution_blocks -> Jsonb,
        relay_id -> Uuid,
        entity_id -> Uuid,
        remote_entity_name -> Varchar,
        needs_subquery_transformation -> Bool,
    }
}

diesel::table! {
    remote_info_mapping (id) {
        id -> Uuid,
        remote_entity_mapping_id -> Uuid,
        information_id -> Uuid,
        info_mapped_name -> Varchar,
        literal_derived_field -> Bool,
        transformation -> Jsonb,
    }
}

diesel::table! {
    user_source_permission (id) {
        id -> Uuid,
        data_source_id -> Uuid,
        user_id -> Uuid,
        source_permission -> Jsonb,
    }
}

diesel::table! {
    users (id) {
        id -> Uuid,
        x509_sha256 -> Varchar,
        x509_subject -> Varchar,
        x509_issuer -> Varchar,
    }
}

diesel::joinable!(data_field -> data_source (data_source_id));
diesel::joinable!(data_source -> data_connection (data_connection_id));
diesel::joinable!(default_source_permission -> data_source (data_source_id));
diesel::joinable!(field_mappings -> data_field (data_field_id));
diesel::joinable!(field_mappings -> information (information_id));
diesel::joinable!(incoming_flight_streams -> query_task_remote (query_task_remote_id));
diesel::joinable!(information -> entities (entity_id));
diesel::joinable!(query_request -> relays (relay_id));
diesel::joinable!(query_task -> data_source (data_source_id));
diesel::joinable!(query_task -> query_request (query_request_id));
diesel::joinable!(query_task_remote -> query_request (query_request_id));
diesel::joinable!(query_task_remote -> relays (relay_id));
diesel::joinable!(relay_source_permission -> data_source (data_source_id));
diesel::joinable!(relay_source_permission -> relays (relay_id));
diesel::joinable!(remote_entity_mapping -> entities (entity_id));
diesel::joinable!(remote_entity_mapping -> relays (relay_id));
diesel::joinable!(remote_info_mapping -> information (information_id));
diesel::joinable!(remote_info_mapping -> remote_entity_mapping (remote_entity_mapping_id));
diesel::joinable!(user_source_permission -> data_source (data_source_id));
diesel::joinable!(user_source_permission -> users (user_id));

diesel::allow_tables_to_appear_in_same_query!(
    data_connection,
    data_field,
    data_source,
    default_source_permission,
    entities,
    field_mappings,
    incoming_flight_streams,
    information,
    query_request,
    query_task,
    query_task_remote,
    relay_source_permission,
    relays,
    remote_entity_mapping,
    remote_info_mapping,
    user_source_permission,
    users,
);
