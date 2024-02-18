#!/bin/bash

mkdir results

set -e

if [[ $1 == "local" ]] 
then
    deploy/build_all_local.sh 
elif [[ $1 == "release" ]]
then
    deploy/build_all_release.sh
else
    deploy/build_all_debug.sh
fi

# These are the server certs for hosted services
mkcert -key-file deploy/development/global_data_relay/key.pem \
-cert-file deploy/development/global_data_relay/cert.pem \
global-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/na_data_relay/key.pem \
-cert-file deploy/development/na_data_relay/cert.pem \
na-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/na_us_data_relay/key.pem \
-cert-file deploy/development/na_us_data_relay/cert.pem \
na-us-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/emea_data_relay/key.pem \
-cert-file deploy/development/emea_data_relay/cert.pem \
emea-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/apac_data_relay/key.pem \
-cert-file deploy/development/apac_data_relay/cert.pem \
apac-data-relay localhost 127.0.0.1

# These are the client certs when authenticating to other relays' services
mkcert -key-file deploy/development/global_data_relay/client_key.pem \
-cert-file deploy/development/global_data_relay/client_cert.pem \
-client \
global-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/na_data_relay/client_key.pem \
-cert-file deploy/development/na_data_relay/client_cert.pem \
-client \
na-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/na_us_data_relay/client_key.pem \
-cert-file deploy/development/na_us_data_relay/client_cert.pem \
-client \
na-us-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/emea_data_relay/client_key.pem \
-cert-file deploy/development/emea_data_relay/client_cert.pem \
-client \
emea-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/apac_data_relay/client_key.pem \
-cert-file deploy/development/apac_data_relay/client_cert.pem \
-client \
apac-data-relay localhost 127.0.0.1

mkcert -key-file deploy/development/global_data_relay/offline_key.pem \
-cert-file deploy/development/global_data_relay/offline_cert.pem \
-client \
offline-relay localhost 127.0.0.1

# These are the client certs for test users authenticating with the relay network
mkcert -key-file client_key_all_access.pem \
-cert-file client_cert_all_access.pem \
-client \
localhost 127.0.0.1

mkcert -key-file client_key_default_access.pem \
-cert-file client_cert_default_access.pem \
-client \
localhost 127.0.0.1

cp $(mkcert -CAROOT)/rootCA.pem ./cacert.pem

find . -type f -name "*.pem" -print0 | xargs -0 chmod 744
find . -type f -name "*.yaml" -print0 | xargs -0 chmod 744

docker compose -f ./deploy/compose.yaml up -d --force-recreate

#give some time for the database to fully come up before attempting to configure
sleep 5

deploy/development/global_data_relay/configure_global_data_relay.sh 
deploy/development/na_data_relay/configure_na_data_relay.sh 
deploy/development/na_us_data_relay/configure_na_us_data_relay.sh 
deploy/development/emea_data_relay/configure_emea_data_relay.sh 
deploy/development/apac_data_relay/configure_apac_data_relay.sh 

