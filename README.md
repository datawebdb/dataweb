# DataWeb

The goal of DataWeb is to enable virtual integration of siloed data systems with minimal central coordination. This means that data remains physically siloed and all engineering teams participating in the DataWeb retain autonomy. There are no enterprise data models, no strict data contracts, no massive data catalogs to manually sift through, and no bulk physical movements of data. Nontheless, the DataWeb presents a unified view of all data in the web which can be queried using locally scoped data models.

### Architecture

A Relay is a node in the DataWeb, which has its own independent set of logical data models. Each Relay is connected to physical data sources and remote logical data sources (i.e. other Relays in the Web). 

<img src='/images/relay.png' width='600'>

Relays are responsible for propagating every logical query recieved, maintaining a set of transformations needed to transform data on the fly into the original requestor's expected data model. The DataWeb itself is not responsible for aggregations or joins which span multiple data sources. Instead, the requestor (which itself can be an execution engine) is responsible for final aggregations/joins on the returned data streams. 

<img src='/images/propagate.png' width='700'>

The original requestor is given a list of endpoints from where it can retrieve all relevant streams of data. This time, it can request directly to any Relay with data, rather than propagating the request indirectly through a single Relay.

<img src='/images/integrate.png' width='700'>

While the Relay network can be arranged in any topology, the expected use case is for the topology to match the org chart of the organization creating the web. Consider the following structure of an organization organized based on regions of the world:

<img src='/images/org_chart.png' width='350'>

Each part of the organization only needs to concern itself with its immediate parent and children. I.e. the US focused part of the org only needs to maintain its own data models and mappings to the North America wide level of the org. Notably, horizontal communication within the organization is not required to construct the web. This fact is intentional in the design of DataWeb, given the difficulty in enforcing collaboration in horizontal slices of large organizations, see [stovepipes](https://en.wikipedia.org./wiki/Stovepipe_(organisation)).

### Data Security + Compartmentalization

Each Relay operates autonomously with zero trust for any other Relay or User in the network. Every connection is authenticated and encrypted via mTLS. Access controls are expressed as arbitrary SQL statements, which enables fine grained control over which columns and rows are exposed to which parts of the organization and even which specific users. No single server or user in the network (even any sever admin) must have access to all data in the web. This limits the blast radius in the event of even the most serious compromise or insider threat scenario.

This security model makes DataWeb Relay appropriate for controling access to the most sensitive data within organziations. It also means DataWeb is appropriate for integrating data accross multiple organizations, where there is no common central data owner. Every data consumer can securely and efficiently query every source of data throughout the Web to which they have been granted access, but **nothing more**. Eliminating the trade off between compartmentalization and efficiently analyzing petabyte scale analytical data is a key objective of this project.

### Open Source Composable Standards

The DataWeb enables efficient, high bandwidth access to siloed analytical data via the open composable standards of [Apache Arrow](https://arrow.apache.org/). In fact, DataWeb Relays are nothing more than a custom [Apache Arrow Flight](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) server. This enables Relays to efficiently exchange extremely large amounts of data over the network efficiently. 

While the Relays internally communicate via a custom query templating language, [DataWeb Engine](/DataWebEngine) implements a DataFusion table provider for queries over the entire DataWeb. This enables data consumers to use familiar SQL queries as though they are querying a single Execution Engine, when in fact they may be querying hundreds to thousands of scattered datasources throughout a complex network of DataWeb Relays.

Each Relay can communicate with the DataSources it controls via FlightSQL. This enables integrating any FlightSQL compliant execution engine into the DataWeb seemlessly and with no special development or plug-ins required.

### Extend the Web to the Edge

Implemented 100% in Rust, DataWeb Relay can be statically compiled down to a single binary approximately 50MB in size. A Relay is resource efficient enough to be deployed at the edge, near the collection source of data. This decreases the time it takes to make data available enterprise wide to essentially zero. For example, a Relay could be deployed directly on an IoT device which stores CSV files on its local storage. This Relay can be configured with an embeded DataFusion query engine to expose this data to the web. As soon as additional data is saved to local storage, it immediately becomes available throughout the Web.

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

Note that the **default permission** is the data which any user which authenticates with a x509 certificate from a trusted CA is granted access to. If you are using a public CA, then this would be **public** data. If you are using a private CA, then this would be the data exposed to anyone who may obtain a certificate within that organization.

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

This completes the mapping from the abstract Customer entity to a queryable local data source. A single Entity can be mapped to an arbitrarily large number of local data sources. Each local data soruce can be arbitrarily transformed to conform to the Entity's schema by modifying the source_sql field to any SQL query. 

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

Here we see a few of the fields are named differently, but are otherwise the same. These differences are invisible to end User's, who only need to express queries in terms of a single local data model. It is the responsibility of the Relay network to transform queries for local data sources and remote relays.

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
