echo "configuring na_data_relay!"

export CLIENT_CERT_FILE='users/client_cert_admin.pem'
export CLIENT_KEY_FILE='users/client_key_admin.pem'
export CA_CERT_FILE='users/cacert.pem'
export RELAY_ENDPOINT='https://localhost:8447'

docker run --network="host" \
--env CLIENT_CERT_FILE \
--env CLIENT_KEY_FILE \
--env RELAY_ENDPOINT \
--env CA_CERT_FILE \
-v $PWD/deploy/development/global_data_relay/:/deploy/development/global_data_relay/ \
-v $PWD/deploy/development/na_data_relay/:/deploy/development/na_data_relay/ \
-v $PWD/deploy/development/na_us_data_relay/:/deploy/development/na_us_data_relay/ \
-v $PWD/:/users/ \
relayctl apply -f deploy/development/na_data_relay/data_modeling
