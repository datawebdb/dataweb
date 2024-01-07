-- Your SQL goes here
CREATE TABLE entities (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE information (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR NOT NULL,
  arrow_dtype jsonb NOT NULL,
  entity_id uuid NOT NULL REFERENCES entities(id),
  UNIQUE (entity_id, name)
);

CREATE TABLE data_connection (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR NOT NULL UNIQUE,
    connection_options jsonb NOT NULL
);

CREATE TABLE data_source (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR NOT NULL,
    source_sql VARCHAR NOT NULL,
    data_connection_id uuid NOT NULL REFERENCES data_connection(id),
    source_options jsonb NOT NULL,
    UNIQUE (data_connection_id, name)
);

CREATE TABLE data_field (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR NOT NULL,
    data_source_id uuid NOT NULL REFERENCES data_source(id),
    path varchar NOT NULL,
    UNIQUE (data_source_id, name)
);

CREATE TABLE field_mappings (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    data_field_id uuid NOT NULL REFERENCES data_field(id),
    information_id uuid NOT NULL REFERENCES information(id),
    transformation jsonb NOT NULL
);

CREATE TABLE relays (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR NOT NULL UNIQUE,
    rest_endpoint VARCHAR NOT NULL,
    flight_endpoint VARCHAR NOT NULL,
    x509_sha256 VARCHAR NOT NULL UNIQUE,
    x509_subject VARCHAR NOT NULL,
    x509_issuer VARCHAR NOT NULL
);

CREATE TABLE users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    x509_sha256 VARCHAR NOT NULL UNIQUE,
    x509_subject VARCHAR NOT NULL,
    x509_issuer VARCHAR NOT NULL
);

CREATE UNIQUE INDEX x509
ON users (x509_sha256);

CREATE TABLE query_request (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    originator_request_id uuid NOT NULL UNIQUE,
    sql VARCHAR NOT NULL,
    relay_id uuid NOT NULL REFERENCES relays(id),
    origin_info jsonb NOT NULL,
    substitution_blocks jsonb NOT NULL
);

CREATE TYPE query_task_status AS ENUM ('queued', 'in_progress', 'complete', 'failed');

CREATE TABLE query_task (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    query_request_id uuid NOT NULL REFERENCES query_request(id),
    data_source_id uuid NOT NULL REFERENCES data_source(id),
    task jsonb NOT NULL,
    status query_task_status NOT NULL
);

CREATE TYPE query_task_remote_status AS ENUM ('queued', 'submitted', 'complete', 'failed');

CREATE TABLE query_task_remote (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    query_request_id uuid NOT NULL REFERENCES query_request(id),
    relay_id uuid NOT NULL REFERENCES relays(id),
    task jsonb NOT NULL,
    status query_task_remote_status NOT NULL
);

CREATE TYPE flight_stream_status AS ENUM ('invalid', 'started', 'failed', 'complete');

CREATE TABLE incoming_flight_streams (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    query_task_remote_id uuid NOT NULL REFERENCES query_task_remote(id),
    remote_fingerprint varchar NOT NULL,
    flight_id uuid NOT NULL UNIQUE,
    status flight_stream_status NOT NULL
);

CREATE TABLE default_source_permission (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    data_source_id uuid NOT NULL REFERENCES data_source(id) UNIQUE,
    source_permission jsonb NOT NULL
);

CREATE TABLE relay_source_permission (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    data_source_id uuid NOT NULL REFERENCES data_source(id),
    relay_id uuid NOT NULL REFERENCES relays(id),
    source_permission jsonb NOT NULL,
    UNIQUE(data_source_id, relay_id)
);

CREATE TABLE user_source_permission (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    data_source_id uuid NOT NULL REFERENCES data_source(id),
    user_id uuid NOT NULL REFERENCES users(id),
    source_permission jsonb NOT NULL,
    UNIQUE(data_source_id, user_id)
);

CREATE TABLE remote_entity_mapping (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sql VARCHAR NOT NULL,
    substitution_blocks jsonb NOT NULL,
    relay_id uuid NOT NULL REFERENCES relays(id),
    entity_id uuid NOT NULL REFERENCES entities(id),
    remote_entity_name varchar not null,
    needs_subquery_transformation bool not null,
    UNIQUE(relay_id, entity_id, remote_entity_name)
);

CREATE TABLE remote_info_mapping (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    remote_entity_mapping_id uuid NOT NULL REFERENCES remote_entity_mapping(id),
    information_id uuid NOT NULL REFERENCES information(id),
    info_mapped_name varchar not null,
    literal_derived_field bool not null,
    transformation jsonb NOT NULL,
    UNIQUE(remote_entity_mapping_id, information_id)
);





