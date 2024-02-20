echo "configuring na_data_relay!"

export DATABASE_URL='postgres://postgres:dev!@localhost:5433/na_data_relay'

docker run --network="host" \
--env RUST_BACKTRACE \
--env RELAY_NAME \
--env DATABASE_URL \
-v $PWD/deploy/development/global_data_relay/:/deploy/development/global_data_relay/ \
-v $PWD/deploy/development/na_data_relay/:/deploy/development/na_data_relay/ \
-v $PWD/deploy/development/na_us_data_relay/:/deploy/development/na_us_data_relay/ \
-v $PWD/:/users/ \
relayctl \
--entity-configs /deploy/development/na_data_relay/data_modeling/local_entities \
--local-data-configs /deploy/development/na_data_relay/data_modeling/local_data_sources \
--local-mapping-configs /deploy/development/na_data_relay/data_modeling/local_data_mappings \
--remote-relay-configs /deploy/development/na_data_relay/data_modeling/remote_relays \
--remote-mapping-configs /deploy/development/na_data_relay/data_modeling/remote_data_mappings \
--user-mapping-configs /deploy/development/na_data_relay/data_modeling/users
