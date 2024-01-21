#!/bin/bash

# builds everything in the workspace
docker build -f deploy/Dockerfile -t release_build --target release_build .

# subsequent builds can pull the binaries from the build image using --target release_prebuild
docker build -f flight_server/Dockerfile -t flight --target release_prebuild .

docker build -f rest_server/Dockerfile -t rest_server --target release_prebuild .

docker build -f query_runner/Dockerfile -t query_runner --target release_prebuild .

docker build -f single_binary_deployment/Dockerfile -t single_bin_deployment --target release_prebuild .

docker build -f relayctl/Dockerfile -t relayctl --target release_prebuild .

docker build -f webengine/Dockerfile -t webengine --target release_prebuild .

docker build -f deploy/Dockerfile -t diesel --target diesel_build .

docker build -f deploy/Dockerfile.Ballista -t ballista-scheduler --target scheduler .

docker build -f deploy/Dockerfile.Ballista -t ballista-executor --target executor .