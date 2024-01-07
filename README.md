# DataWeb Relay

DataWeb Relay enables connecting siloed data systems with minimal central coordination and the strictest data security standards. A network of connected Relays forms a unified DataWeb, which can be queried as though it were a single central datastore. This enables organizations to horizontally scale Data Engineering teams, while still enabling Data Analysts to easily discover and query all relevant data, all while keeping sensitive data highly compartmentalized. 

### Data Security + Compartmentalization

Each Relay operates autonomously with **zero trust** for any other Relay or User in the network. Every connection is authenticated and encrypted via mTLS and each Relay can control which other Relays and Users can access which Columns and Rows of any DataSource it exposes to the network. Authorization controls can be expressed as arbitrary SQL statements, which are aware of who is requesting the data, what part of the organization they are part of, and more. No single server or user in the network (even any sever admin) must have access to all or even a plurality of the data in the organization. This limits the blast radius in the event of even the most serious compromise or insider threat scenario.

This security model makes DataWeb Relay appropriate for controling access to the most sensitive data within organziations. It also means DataWeb Relay is appropriate for connecting data accross organizations, where there is no common central data owner. Every analyst can securely and efficiently query every source of data throughout the Web to which they have been granted access, but **nothing more**. Eliminating the trade off between data security, compartmentalization, and efficiently exploiting petabyte to exabyte scale analytical data is the key objective of this project.

### Open Source Composable Standards

The DataWeb enables efficient, high bandwidth access to compartmented analytical data via the open composable standards of [Apache Arrow](https://arrow.apache.org/). In fact, DataWeb Relay is nothing more than a highly customized and hardened [Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) server. Relays communicate via standard Flight gRPC calls, such as get_flight_info and do_get. This enables Relays to efficiently exchange extremely large amounts of data over the network efficiently. 

While the Relays themselves communicate via a custom query templating language, [DataWeb Engine](https://github.com/devinjdangelo/DataWebEngine) implements a DataFusion table provider and FlightSQL compliant API for queries over the entire DataWeb. This enables analysts to use familiar ANSI-SQL queries as though they are querying a single ExecutionEngine, when in fact they may querying hundreds to thousands of scattered datasources throughout a complex mesh network of DataWeb Relays.

Each Relay can communicate with the DataSources it controls via FlightSQL as well. This enables integrating any FlightSQL compliant execution engine into the DataWeb seemlessly and with no special development or plug-ins required.

### Extend the Web to the Edge

Implemented 100% in Rust, DataWeb Relay can be statically compiled down to a single binary just 53MB in size. A Relay is resource efficient enough to be deployed at the edge, near the collection source of data. This decreases the time it takes to make data available enterprise wide to essentially zero. For example, a Relay could be deployed directly on an IoT device which stores CSV files on its local storage. This Relay can be configured with an embeded DataFusion query engine to expose this data to the web. As soon as additional data is saved to local storage, it immediately becomes available throughout the Web.

Data Analysts do not need to be aware of or take any manual action to discover new data in the web. If new data is added to the web which is relevant to their query, it will automatically be discovered by the Relay network and added to the results. This eliminates the need for analysts to manually search massive data catalogs and decide which data is relevant themselves.

### Configuration

Each Relay defines virtual Arrow Schemas called an "Entity". Using the included relayctl cli tool, these and all other configurations can be defined via a declarative YAML files. A simple example follows, but see also a more complex Web defined for integration testing purposes in [this folder](deploy/development).

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
  - name: tpch.sf1.customer
    source_sql: tpch.sf1.customer
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

Note that the **default permission** is the data which any user which authenticates with a x509 certificate from a trusted CA is granted access to. If you are using a public CA, then this would be **public** data. If you are using a private CA, then this would be the data exposed to anyone who may obtain a cert within that organization.

```yaml
entity_name: customer
mappings:
  - data_con_name: trino_tpch
    source_mappings:
    - data_source_name: tpch.sf1.customer
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

This completes the mapping from the abstract Customer entity to a queryable local data source. A single Entity can be mapped to an arbitrarily large number of local data sources. Each local data soruce can be arbitrarily transparently transformed to conform to the Entity's schema by modifying the source_sql field to any SQL query. 

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

Here we see a few of the fields are named differently, but are otherwise the same. These differences are invisible to end User's, who only need to express queries in terms of a single data model. It is the responsible of the Relay network to transform queries for local DataSources and remote Relays.

### Development and Testing

Unit tests can be run the usual way via cargo:

```
cargo test
```

Integration testing is important and complex, since much of the behavior we want to test is based on how a web of Relays will interact with each other. A development/testing Web with 6 Relays configured with TPCH related data and models can be deployed via Docker with a single command:

```
deploy/build_and_deploy.sh
```

Pyarrow based low-level integration tests can be executed via:

```
pytest
```

Additional more complex end-to-end tests can be run by using the [DataWeb Engine](https://github.com/devinjdangelo/DataWebEngine). 
