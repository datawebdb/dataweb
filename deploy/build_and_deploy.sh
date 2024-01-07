#!/bin/bash

#blow away any existing deployment if exists
docker stop postgres
docker stop trino
docker stop global_data_relay
docker stop na_data_relay
docker stop na_us_data_relay
docker stop emea_data_relay 
docker stop apac_data_relay 
docker rm global_data_relay
docker rm na_data_relay
docker rm na_us_data_relay
docker rm emea_data_relay 
docker rm apac_data_relay 
docker rm postgres
docker rm trino

mkdir results

set -e

docker run --name trino -d -p 8080:8080 trinodb/trino
docker run --name postgres --network="host" -e POSTGRES_PASSWORD=dev!@ -d postgres

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
localhost 127.0.0.1

mkcert -key-file deploy/development/global_data_relay/key.pem \
-cert-file deploy/development/global_data_relay/cert.pem \
localhost 127.0.0.1

mkcert -key-file deploy/development/na_data_relay/key.pem \
-cert-file deploy/development/na_data_relay/cert.pem \
localhost 127.0.0.1

mkcert -key-file deploy/development/na_us_data_relay/key.pem \
-cert-file deploy/development/na_us_data_relay/cert.pem \
localhost 127.0.0.1

mkcert -key-file deploy/development/emea_data_relay/key.pem \
-cert-file deploy/development/emea_data_relay/cert.pem \
localhost 127.0.0.1

mkcert -key-file deploy/development/apac_data_relay/key.pem \
-cert-file deploy/development/apac_data_relay/cert.pem \
localhost 127.0.0.1

# These are the client certs when authenticating to other relays' services
mkcert -key-file deploy/development/global_data_relay/client_key.pem \
-cert-file deploy/development/global_data_relay/client_cert.pem \
-client \
localhost 127.0.0.1

mkcert -key-file deploy/development/na_data_relay/client_key.pem \
-cert-file deploy/development/na_data_relay/client_cert.pem \
-client \
localhost 127.0.0.1

mkcert -key-file deploy/development/na_us_data_relay/client_key.pem \
-cert-file deploy/development/na_us_data_relay/client_cert.pem \
-client \
localhost 127.0.0.1

mkcert -key-file deploy/development/emea_data_relay/client_key.pem \
-cert-file deploy/development/emea_data_relay/client_cert.pem \
-client \
localhost 127.0.0.1

mkcert -key-file deploy/development/apac_data_relay/client_key.pem \
-cert-file deploy/development/apac_data_relay/client_cert.pem \
-client \
localhost 127.0.0.1

mkcert -key-file deploy/development/global_data_relay/offline_key.pem \
-cert-file deploy/development/global_data_relay/offline_cert.pem \
-client \
localhost 127.0.0.1

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

deploy/development/global_data_relay/deploy_global_data_relay.sh 
deploy/development/na_data_relay/deploy_na_data_relay.sh 
deploy/development/na_us_data_relay/deploy_na_us_data_relay.sh 
deploy/development/emea_data_relay/deploy_emea_data_relay.sh 
deploy/development/apac_data_relay/deploy_apac_data_relay.sh 

