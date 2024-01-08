# DataWebEngine

Implements a DataFusion `TableProvider` for a DataWeb `Entity`. This enables arbitrary aggregations and joins over `Entites` which each may be composed of dozens to hundreds of fragments spread throughout the web.

Currently, this is only used for high level integration tests which verify TPCH queries over a web. To run these integration tests, run the following from the repository root:

```
cargo run -p data_web_engine
```