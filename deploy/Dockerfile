# Build Stage
FROM ubuntu:22.04 AS debug_builder

RUN apt-get update && apt-get install curl build-essential libpq-dev -y
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /usr/src/
COPY ./ ./
RUN cargo build

# copy out the binaries
FROM scratch as debug_build
COPY --from=debug_builder /usr/src/target/debug/flight_server /
COPY --from=debug_builder /usr/src/target/debug/rest_server /
COPY --from=debug_builder /usr/src/target/debug/query_runner /
COPY --from=debug_builder /usr/src/target/debug/single_binary_deployment /
COPY --from=debug_builder /usr/src/target/debug/relayctl /
COPY --from=debug_builder /usr/src/target/debug/data_web_engine /

#copy binaries from a local build
FROM scratch as debug_local_build
COPY ./target/debug/flight_server /
COPY ./target/debug/rest_server /
COPY ./target/debug/query_runner /
COPY ./target/debug/single_binary_deployment /
COPY ./target/debug/relayctl /
COPY ./target/debug/data_web_engine /

# Build Stage
FROM rust:1.76.0-bullseye AS release_builder
WORKDIR /usr/src/

COPY ./ ./
RUN cargo build --release

# copy out the binaries
FROM debian:11 as release_build
COPY --from=release_builder /usr/src/target/release/flight_server /
COPY --from=release_builder /usr/src/target/release/rest_server /
COPY --from=release_builder /usr/src/target/release/query_runner /
COPY --from=release_builder /usr/src/target/release/single_binary_deployment /
COPY --from=release_builder /usr/src/target/release/relayctl /
COPY --from=release_builder /usr/src/target/release/data_web_engine /