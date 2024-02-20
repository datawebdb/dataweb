echo "configuring apac_data_relay!"

export DATABASE_URL='postgres://postgres:dev!@localhost:5436/apac_data_relay'

docker run --network="host" \
--env RUST_BACKTRACE \
--env RELAY_NAME \
--env DATABASE_URL \
-v $PWD/deploy/development/global_data_relay/:/deploy/development/global_data_relay/ \
-v $PWD/deploy/development/apac_data_relay/:/deploy/development/apac_data_relay/ \
-v $PWD/deploy/development/emea_data_relay/:/deploy/development/emea_data_relay/ \
-v $PWD/:/users/ \
relayctl \
--entity-configs deploy/development/apac_data_relay/data_modeling/local_entities \
--local-data-configs deploy/development/apac_data_relay/data_modeling/local_data_sources \
--local-mapping-configs deploy/development/apac_data_relay/data_modeling/local_data_mappings \
--remote-relay-configs deploy/development/apac_data_relay/data_modeling/remote_relays \
--remote-mapping-configs deploy/development/apac_data_relay/data_modeling/remote_data_mappings \
--user-mapping-configs deploy/development/apac_data_relay/data_modeling/users


