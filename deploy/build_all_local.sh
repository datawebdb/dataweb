#!/bin/bash

# builds everything in the workspace
cargo zigbuild --target x86_64-unknown-linux-musl
docker build -f deploy/Dockerfile -t debug_build --target debug_local_build .

# subsequent builds can pull the binaries from the build image using --target prebuild
docker build -f flight_server/Dockerfile -t flight_debug --target debug_prebuild .

docker build -f rest_server/Dockerfile -t rest_debug --target debug_prebuild .

docker build -f query_runner/Dockerfile -t query_runner_debug --target debug_prebuild .

docker build -f single_binary_deployment/Dockerfile -t single_bin_deployment --target debug_prebuild .

docker build -f relayctl/Dockerfile -t relayctl --target debug_prebuild .

docker build -f webengine/Dockerfile -t webengine --target debug_prebuild .

docker build -f deploy/Dockerfile -t diesel --target diesel_build .

docker build -f deploy/Dockerfile.Ballista -t ballista-scheduler --target scheduler .

docker build -f deploy/Dockerfile.Ballista -t ballista-executor --target executor .