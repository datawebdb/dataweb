# Build Stage
FROM rust:1.73.0 AS debug_builder
WORKDIR /usr/src/

RUN apt-get update 
RUN apt-get install musl musl-tools python3-pip -y 
RUN pip3 install cargo-zigbuild --break-system-packages
RUN rustup target add x86_64-unknown-linux-musl


COPY ./ ./
RUN cargo zigbuild --target x86_64-unknown-linux-musl

# copy out the binaries
FROM scratch as debug_build
COPY --from=debug_builder /usr/src/target/x86_64-unknown-linux-musl/debug/flight_server /
COPY --from=debug_builder /usr/src/target/x86_64-unknown-linux-musl/debug/rest_server /
COPY --from=debug_builder /usr/src/target/x86_64-unknown-linux-musl/debug/query_runner /
COPY --from=debug_builder /usr/src/target/x86_64-unknown-linux-musl/debug/single_binary_deployment /
COPY --from=debug_builder /usr/src/target/x86_64-unknown-linux-musl/debug/relayctl /

#copy binaries from a local build
FROM scratch as debug_local_build
COPY ./target/x86_64-unknown-linux-musl/debug/flight_server /
COPY ./target/x86_64-unknown-linux-musl/debug/rest_server /
COPY ./target/x86_64-unknown-linux-musl/debug/query_runner /
COPY ./target/x86_64-unknown-linux-musl/debug/single_binary_deployment /
COPY ./target/x86_64-unknown-linux-musl/debug/relayctl /

# Build Stage
FROM rust:1.73.0 AS release_builder
WORKDIR /usr/src/

RUN apt-get update 
RUN apt-get install musl musl-tools python3-pip -y 
RUN pip3 install cargo-zigbuild --break-system-packages
RUN rustup target add x86_64-unknown-linux-musl

COPY ./ ./
RUN cargo zigbuild --target x86_64-unknown-linux-musl --release

# copy out the binaries
FROM scratch as release_build
COPY --from=release_builder /usr/src/target/x86_64-unknown-linux-musl/release/flight_server /
COPY --from=release_builder /usr/src/target/x86_64-unknown-linux-musl/release/rest_server /
COPY --from=release_builder /usr/src/target/x86_64-unknown-linux-musl/release/query_runner /
COPY --from=release_builder /usr/src/target/x86_64-unknown-linux-musl/release/single_binary_deployment /
COPY --from=release_builder /usr/src/target/x86_64-unknown-linux-musl/release/relayctl /

FROM rust:1.73.0 AS diesel_build
RUN apt-get update 
RUN apt-get install libpq-dev -y
RUN cargo install diesel_cli --no-default-features --features postgres
ENTRYPOINT [ "diesel" ]