echo "starting global_data_relay!"

export RELAY_NAME="global_data_relay"
export DATABASE_URL='postgres://postgres:dev!@localhost/global_data_relay'
export RESULT_SOURCE_OBJECT_STORE="LocalFileSystem"
export RESULT_SOURCE_PFX="${PWD}/results/"
export FLIGHT_SERVICE_ENDPOINT="0.0.0.0:50051"
export MSG_BROKER_OPTS='{"type": "AsyncChannel"}'
export REST_SERVICE_URL="0.0.0.0"
export REST_SERVICE_PORT="8443"
export CA_CERT_FILE=${PWD}/cacert.pem
export SERVER_CERT_FILE=${PWD}/deploy/development/global_data_relay/cert.pem 
export SERVER_KEY_FILE=${PWD}/deploy/development/global_data_relay/key.pem
export CLIENT_CERT_FILE=${PWD}/deploy/development/global_data_relay/client_cert.pem 
export CLIENT_KEY_FILE=${PWD}/deploy/development/global_data_relay/client_key.pem
export MIN_PARALLELISM_PER_QUERY_WORKER=8
export RUST_LOG=4

docker run --network="host" \
--env DATABASE_URL \
-v $PWD/core/migrations:/migrations \
diesel setup 

docker run --network="host" \
--env DATABASE_URL \
-v $PWD/core/migrations:/migrations \
diesel database reset

docker run --network="host" \
--env DATABASE_URL \
-v $PWD/core/migrations:/migrations \
diesel migration run 

docker run --network="host" \
--env DATABASE_URL \
-v $PWD/core/migrations:/migrations \
diesel migration redo --all 

docker run --network="host" \
--env RUST_BACKTRACE \
--env RELAY_NAME \
--env DATABASE_URL \
-v $PWD/deploy/development/apac_data_relay/:/deploy/development/apac_data_relay/ \
-v $PWD/deploy/development/emea_data_relay/:/deploy/development/emea_data_relay/ \
-v $PWD/deploy/development/global_data_relay/:/deploy/development/global_data_relay/ \
-v $PWD/deploy/development/na_data_relay/:/deploy/development/na_data_relay/ \
-v $PWD/:/users/ \
relayctl \
--entity-configs deploy/development/global_data_relay/data_modeling/local_entities \
--local-data-configs deploy/development/global_data_relay/data_modeling/local_data_sources \
--local-mapping-configs deploy/development/global_data_relay/data_modeling/local_data_mappings \
--remote-relay-configs deploy/development/global_data_relay/data_modeling/remote_relays \
--remote-mapping-configs deploy/development/global_data_relay/data_modeling/remote_data_mappings \
--user-mapping-configs deploy/development/global_data_relay/data_modeling/users

docker run -d \
--network="host" \
--env RELAY_NAME \
--env DATABASE_URL \
--env RESULT_SOURCE_OBJECT_STORE \
--env RESULT_SOURCE_PFX \
--env FLIGHT_SERVICE_ENDPOINT \
--env MSG_BROKER_OPTS \
--env REST_SERVICE_URL \
--env REST_SERVICE_PORT \
--env CA_CERT_FILE \
--env SERVER_CERT_FILE \
--env SERVER_KEY_FILE \
--env CLIENT_CERT_FILE \
--env CLIENT_KEY_FILE \
--env RUST_LOG \
--env MIN_PARALLELISM_PER_QUERY_WORKER \
-v ${PWD}:${PWD} \
-w ${PWD} \
--name global_data_relay \
single_bin_deployment

