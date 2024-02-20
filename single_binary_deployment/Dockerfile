# Build Stage - used to build ONLY the single_bin_deployment image
FROM rust:1.76.0-bullseye AS debug_builder
WORKDIR /usr/src/

COPY ./ ./
RUN cargo build -p single_binary_deployment

#Bundle Stage if using solo build of single_bin_deployment
FROM ubuntu:22.04 as debug_rebuild
COPY --from=debug_builder /usr/src/target/debug/single_binary_deployment /
RUN apt-get update && apt-get install libpq-dev -y
USER 1000
CMD ["/single_binary_deployment"]

#References the base build image, which contains prebuild binaries of all system components
FROM debug_build as debug_build

#Copy the prebuild binary from the base build image
FROM ubuntu:22.04 as debug_prebuild
COPY --from=debug_build /single_binary_deployment /
RUN apt-get update && apt-get install libpq-dev -y
CMD ["/single_binary_deployment"]

# Build Stage - used to build ONLY the single_bin_deployment image
FROM rust:1.76.0-bullseye AS release_builder
WORKDIR /usr/src/

COPY ./ ./
RUN cargo build -p single_binary_deployment --release

#Bundle Stage if using solo build of single_bin_deployment
FROM debian:11 as release_rebuild
RUN apt-get update && apt-get install libpq-dev -y
COPY --from=release_builder /usr/src/target/release/single_binary_deployment /
USER 1000
CMD ["/single_binary_deployment"]

#References the base build image, which contains prebuild binaries of all system components
FROM release_build as release_build

#Copy the prebuild binary from the base build image
FROM debian:11 as release_prebuild
COPY --from=release_build /single_binary_deployment /
RUN apt-get update && apt-get install libpq-dev -y
CMD ["/single_binary_deployment"]


