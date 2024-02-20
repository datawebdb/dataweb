# Build Stage - used to build ONLY the query_runner image
FROM rust:1.76.0-bullseye AS debug_builder
WORKDIR /usr/src/

RUN apt-get update 
RUN apt-get install musl musl-tools python3-pip -y 
RUN pip3 install cargo-zigbuild --break-system-packages
RUN rustup target add x86_64-unknown-linux-musl

COPY ./ ./
RUN cargo build -p query_runner

#Bundle Stage if using solo build of query_runner
FROM ubuntu:22.04 as debug_rebuild
COPY --from=debug_builder /usr/src/target/debug/query_runner /
RUN apt-get update && apt-get install libpq-dev -y
USER 1000
CMD ["/query_runner"]

# Build Stage - used to build ONLY the query_runner image
FROM rust:1.76.0-bullseye AS release_builder
WORKDIR /usr/src/

RUN apt-get update 
RUN apt-get install musl musl-tools python3-pip -y 
RUN pip3 install cargo-zigbuild --break-system-packages
RUN rustup target add x86_64-unknown-linux-musl

COPY ./ ./
RUN cargo build -p query_runner --release

#Bundle Stage if using solo build of query_runner
FROM debian:11 as release_rebuild
COPY --from=release_builder /usr/src/target/release/query_runner /
RUN apt-get update && apt-get install libpq-dev -y
USER 1000
CMD ["/query_runner"]


#References the base build image, which contains prebuild binaries of all system components
FROM debug_build as debug_build

#Copy the prebuild binary from the base build image
FROM ubuntu:22.04 as debug_prebuild
COPY --from=debug_build /query_runner /
RUN apt-get update && apt-get install libpq-dev -y
USER 1000
CMD ["/query_runner"]

#References the base build image, which contains prebuild binaries of all system components
FROM release_build as release_build

#Copy the prebuild binary from the base build image
FROM debian:11 as release_prebuild
COPY --from=release_build /query_runner /
RUN apt-get update && apt-get install libpq-dev -y
USER 1000
CMD ["/query_runner"]