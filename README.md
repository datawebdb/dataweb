# DataWeb

DataWeb enables virtual integration of siloed data systems with minimal central coordination. This means that data remains physically siloed and all engineering teams participating in the DataWeb retain autonomy. There are no enterprise data models, no strict data contracts, no massive data catalogs to manually sift through, and no bulk physical movements of data. Nonetheless, the DataWeb presents a unified view of all data in the web which can be easily queried using locally scoped data models.

## Who is this for?

DataWeb is designed for organizations which cannot effectively integrate all of their data in a single central data system (i.e. Data Warehouse, Data Lake, ...), for reasons such as:

* Operational complexity is too high for a central engineering team to handle without bottlenecking
* GDPR or other legal requirements prevent all data from being physically colocated
* Multiple sub-organizations own different pieces of the data and there is no central authority which can own all of it
* Data is too sensitive to risk any single engineering team having access to all of it

If none of these or similar constraints apply to you, then DataWeb is likely unnecessary. If you do face these constraints, then DataWeb can help achieve the same benefits of a single central store by virtually integrating many data systems.

## How it works

A Relay is a node in the DataWeb that has its own independent set of logical data models. Each Relay is connected to physical data sources and remote logical data sources (i.e. other Relays in the Web). 

<img src='/images/relay.png' width='600'>

The first step in querying the DataWeb is sending a query to a Relay specified in terms of that Relay's logical data models. This triggers a query mapping and propagation process, whereby every Relay in the network identifies all relevant data in the Web and computes the necessary transformations to convert all data to the requester's data model. The network tracks and composes transformations upon each hop to enable mapping schemas even in cases where the Relay with relevant data is not directly connected to the originating Relay. 

<img src='/images/propagate.png' width='700'>

At the end of the first step, the original requester is returned a list of endpoints from which it can retrieve all relevant streams of data. This time, it can send each request directly to the relevant Relay, rather than propagating the request indirectly through a single Relay. In the following example query, three relays return a partial sum called "revenue". The execution engine then computes the final sum and returns to the end user the grand total "revenue". 

<img src='/images/integrate.png' width='700'>

While the Relay network can be arranged in any topology, it is recommended to mirror the org chart of the organization creating the web. In other words, scale the DataWeb in parallel to how you scale your organization. Consider the following structure of an organization organized based on regions of the world:

<img src='/images/org_chart.png' width='350'>

Each Relay in this DataWeb only needs to concern itself with its immediate parent and children. I.e. the US focused part of the org only needs to maintain its own data models and mappings to the North America wide level of the org. Notably, horizontal communication within the organization is not required to construct the web. This fact is intentional in the design of DataWeb, given the difficulty in enforcing collaboration in horizontal slices of large organizations, see [stovepipes](https://en.wikipedia.org./wiki/Stovepipe_(organisation)).

### Data Security + Compartmentalization

Each Relay operates autonomously with zero trust for any other Relay or user in the network. Every connection is authenticated and encrypted via mTLS. Access controls are expressed as arbitrary SQL statements, which enables fine grained control over which columns and rows are exposed to which parts of the organization and even to which specific users. No single server or user in the network (or even any server admin) must have access to all data in the web. This limits the blast radius in the event of even the most serious compromise or insider threat scenario.

This security model makes DataWeb Relay appropriate for controlling access to the most sensitive data within organizations. It also means DataWeb is appropriate for integrating data across multiple organizations, where there is no common central data owner. Every data consumer can securely and efficiently query every source of data throughout the Web to which they have been granted access, but nothing more. Eliminating the tradeoff between compartmentalization and efficiently analyzing petabyte scale analytical data is a key objective of this project.

### Open Source Standards

The DataWeb enables efficient, high bandwidth access to siloed analytical data via the open standards of [Apache Arrow](https://arrow.apache.org/). In fact, DataWeb Relays are little more than a custom [Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) server. This enables Relays to efficiently exchange extremely large amounts of data over the network. 

[DataWeb Engine](/webengine) implements a DataFusion table provider for queries over the entire DataWeb. This enables data consumers to use familiar SQL queries as though they are querying a single Execution Engine, when in fact they may be querying hundreds to thousands of scattered data sources throughout a complex network of DataWeb Relays. This includes support for aggregations and joins across multiple Relays.

### Supported Data Sources

There are three ways to connect data to a DataWeb Relay.

* Remote [FlightSQL](https://arrow.apache.org/docs/format/FlightSql.html) Endpoints
* Remote [Trino](https://trino.io/) Clusters
* Embedded [DataFusion](https://arrow.apache.org/datafusion/)

Any external execution engine that implements the FlightSQL protocol can be connected to the web without requiring any special connectors. Since the DataWeb uses the Arrow memory format to communicate internally, FlightSQL is also the most performant protocol for connecting data to the web. 

Given the prevalence of Trino and its large number of supported [connectors](https://trino.io/docs/current/connector.html), Relays contain special logic to enable querying Trino and converting the returned data streams to Arrow memory format. This is the only planned custom integration and FlightSQL should be the strongly preferred method for integrating any data into the web.

The final method of integrating data into the web is for the Relay to act directly as the execution engine by embedding DataFusion. Currently, this allows adding any collection of Parquet, CSV, or JSON files stored locally or in AWS, GCP, or Azure Object Storage.

### Extend the Web to the Edge

Implemented 100% in Rust, DataWeb Relay can be statically compiled down to a single binary approximately 50MB in size. A Relay is resource efficient enough to be deployed at the edge, near the collection source of data. This decreases the time it takes to make data available enterprise wide to essentially zero. For example, a Relay could be deployed directly on an IoT device that stores CSV files on its local storage. This Relay can be configured with an embedded DataFusion query engine to expose this data to the web. As soon as additional data is saved to local storage, it immediately becomes available throughout the Web.

### Deployment

A fully functioning Relay deployment depends on the following components

* (required) Flight server
* (required) PostgreSQL Database
* (optional) REST server
* (optional) Asynchronous Query Runner

The following table describes environment variables which affect deployment. 

Env Var |	Purpose |	Example Value
--- | --- | ---
RELAY_NAME |	Human Readable name for this Relay |	na_relay
DATABASE_URL |	PostgreSQL Database URL	| postgres://postgres:pass@localhost/na_relay
FLIGHT_SERVICE_ENDPOINT | Address where the Flight TLS endpoints are hosted | "0.0.0.0:50055"
CA_CERT_FILE | Bundle of trusted CA certificates in pem format | "./cacert.pem"
SERVER_CERT_FILE | Public x509 certificate for this Relay in pem format | "cert.pem"
SERVER_KEY_FILE | Private key corresponding to SERVER_CERT_FILE | "key.pem"
CLIENT_CERT_FILE | Public x509 certificate used by this Relay when authenticating as a client | "client_cert.pem"
CLIENT_KEY_FILE | Private key corresponding to CLIENT_CERT_FILE | "client_key.pem"
RESULT_SOURCE_OBJECT_STORE | The object store where temporary query results are stored during asynchronous execution | "S3"
RESULT_SOURCE_REGION | The region of the bucket where temporary query results are stored during asynchronous execution | "us-east-1"
RESULT_SOURCE_BUCKET | The bucket where temporary query results are stored during asynchronous execution | "relay_result_bucket"
RESULT_SOURCE_PFX | The prefix within the bucket where temporary query results are stored during asynchronous execution | "/results"
REST_SERVICE_URL | The address where the REST TLS endpoints are hosted | "0.0.0.0"
REST_SERVICE_PORT | The port where the REST TLS endpoints are hosted | "8447"
MSG_BROKER_OPTS | Configuration options related to communication to the Query Runner service during asynchronous query execution (see [MessageBrokerOptions](https://github.com/devinjdangelo/DataWeb/blob/83920ecb96c1268547828b357f6875bff5c0cd41/core/src/messaging/mod.rs#L54)) | '{"type": "AsyncChannel"}'

Services can be deployed independently or as a single binary using `single_binary_deployment`. E.g.

```bash
# Deploy only the flight_server
cargo run -p flight_server
# Deploy only the rest_server
cargo run -p rest_server
# Deploy only the query_runner
cargo run -p query_runner

# Deploy all 3 of the above in 1 binary
cargo run -p single_binary_deployment
```

### Configuration

Each Relay defines virtual Arrow Schemas called an "Entity". Using the included `relayctl` cli tool, these and all other configurations can be defined via declarative YAML files. A simple example follows, but see also a more complex Web defined for integration testing purposes in [this folder](deploy/development).

```yaml
name: customer
information:
  - name: customerkey
    arrow_dtype: Int64
  - name: customername
    arrow_dtype: Utf8
  - name: address
    arrow_dtype: Utf8
  - name: nationkey
    arrow_dtype: Int64
  - name: phone
    arrow_dtype: Utf8
  - name: acctbal
    arrow_dtype: Float64
  - name: mktsegment
    arrow_dtype: Utf8
  - name: comment
    arrow_dtype: Utf8
```

This abstract model can then be mapped to physical data models, i.e. actual physical data or tables within other execution engines the Relay can connect to.

```yaml
name: trino_tpch
connection_options: !Trino
  user: trino
  password: '' #<-- Note that this is the environment variable that will hold the password, not the plaintext password itself
  host: localhost
  port: 8080
  secure: false
data_sources:
  - name: tpch.tiny.customer
    source_sql: tpch.tiny.customer
    source_options: !Trino
    fields:
      - name: custkey
        path: custkey
      - name: name
        path: name
      - name: address
        path: address
      - name: nationkey
        path: nationkey
      - name: phone
        path: phone
      - name: acctbal
        path: acctbal
      - name: mktsegment
        path: mktsegment
      - name: comment
        path: comment
    default_permission:
      allowed_columns:
        - custkey
        - name
        - nationkey
        - acctbal
        - mktsegment
        - comment
      allowed_rows: acctbal>0
```

Note that the **default permission** is the data that any user who authenticates with a x509 certificate from a trusted CA is granted access. If you are using a public CA, then this would be **public** data. If you are using a private CA, then this would be the data exposed to anyone who can obtain a certificate within that organization.

```yaml
entity_name: customer
mappings:
  - data_con_name: trino_tpch
    source_mappings:
    - data_source_name: tpch.tiny.customer
      field_mappings:
        - field: custkey
          info: customerkey
        - field: name
          info: customername
        - field: address
          info: address
        - field: nationkey
          info: nationkey
        - field: phone
          info: phone
        - field: acctbal
          info: acctbal
        - field: mktsegment
          info: mktsegment
        - field: comment
          info: comment
```

This completes the mapping from the abstract Customer entity to a queryable local data source. A single Entity can be mapped to an arbitrarily large number of local data sources. Each local data source can be arbitrarily transformed to conform to the Entity's schema by modifying the source_sql field to any SQL query. 

Where the Relay becomes very powerful is by connecting it to other Peer Relays. Each peer Relay must be declared and named.

```yaml
- name: global_data_relay
  rest_endpoint: "https://localhost:8443"
  flight_endpoint: "https://localhost:50051"
  x509_cert_file: ./deploy/development/global_data_relay/client_cert.pem
```

Here the "client_cert.pem" referenced is the public key of the global_data_relay for when it connects to the relay we are configuring. Now we can map our local abstract data model to the equivalent abstract data model on the remote relay.

```yaml
entity_name: customer
mappings:
  - relay_name: global_data_relay
    remote_entity_name: customer
    relay_mappings:
      - local_info: customerkey
        info_mapped_name: custkey
      - local_info: customer
        info_mapped_name: name
      - local_info: address
        info_mapped_name: address
      - local_info: nationkey
        info_mapped_name: nationkey
      - local_info: phone
        info_mapped_name: phone
      - local_info: acctbal
        info_mapped_name: acctbal
      - local_info: mktsegment
        info_mapped_name: mktsegment
      - local_info: comment
        info_mapped_name: comment
```

Here we see a few of the fields are named differently, but are otherwise the same. These differences are invisible to end users, who only need to express queries in terms of a single local data model. It is the responsibility of the Relay network to transform queries for local data sources and remote relays.

User permissions can also be declared. This is only required if an individual user's permissions need to be elevated above the default.

```yaml
- x509_cert_file: users/client_cert_all_access.pem
  permissions:
    - data_con_name: trino_tpch
      source_permissions:
        - data_source_name: tpch.tiny.customer
          allowed_columns:
            - custkey
            - name
            - address
            - nationkey
            - phone
            - acctbal
            - mktsegment
            - comment
          allowed_rows: true
```

Once all YAML files are defined, a Relay can be configured with them by executing:

```bash
export DATABASE_URL=postgres://postgres:pass@localhost/na_relay
relayctl \
--entity-configs path/to/local_entities \
--local-data-configs path/to/local_data_sources \
--local-mapping-configs path/to/local_data_mappings \
--remote-relay-configs path/to/remote_relays \
--remote-mapping-configs path/to/remote_data_mappings \
--user-mapping-configs path/to/users
```

To update the configuration, simply update the YAML files and rerun the above command.

### Querying the Web

[DataWeb Engine](/webengine) enables querying DataWeb Entities as SQL tables using DataFusion. 

First, establish a connection to any Relay using a valid x509 certificate. The certificate used will affect the columns are rows you are able to access.

```rust
let client_cert = Arc::new(read_pem(&format!("./client_cert.pem")).unwrap());
let client_key = Arc::new(read_pem(&format!("./client_key.pem")).unwrap());
let ca_cert = Arc::new(read_pem("./cacert.pem").unwrap());
let local_relay_endpoint = "https://localhost:50055";

let ctx = SessionContext::new();
register_web_sources(
    &ctx,
    Arc::new(local_relay_endpoint.to_string()),
    client_cert.clone(),
    client_key.clone(),
    ca_cert.clone(),
)
.await?;
```

Then, execute any SQL query treating entity names as a table identifiers.

```rust
let df = ctx.sql("select sum(acctbal) as balance from customer where nationkey='123'").await?;
df.show().await?;
```
```
+---------+
| balance |
+---------+
| 34567   |
+---------+
```

There is full support for joins and aggregations spanning multiple Entities. See this [example](webengine/src/main.rs) with more complex queries used in integration testing.

### Development and Testing

Unit tests can be run the usual way via cargo:

```
cargo test
```

Integration testing is important and complex, since much of the behavior we want to test is based on how a web of Relays will interact with each other. A development/testing Web with 6 Relays configured with TPCH related data and models can be deployed via Docker with a single command:

```
deploy/build_and_deploy.sh
```

This script depends on a local installation of docker and [mkcert](https://github.com/FiloSottile/mkcert).

Pyarrow based low-level integration tests can be executed via:

```
pytest
```

Higher level integration tests can be executed via:

```
cargo run -p data_web_engine
```
