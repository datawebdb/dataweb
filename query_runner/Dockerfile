# Build Stage - used to build ONLY the query_runner image
FROM rust:1.76.0 AS debug_builder
WORKDIR /usr/src/

RUN apt-get update 
RUN apt-get install musl musl-tools python3-pip -y 
RUN pip3 install cargo-zigbuild --break-system-packages
RUN rustup target add x86_64-unknown-linux-musl

COPY ./ ./
RUN cargo zigbuild --target x86_64-unknown-linux-musl -p query_runner

#Bundle Stage if using solo build of query_runner
FROM scratch as debug_rebuild
COPY --from=debug_builder /usr/src/target/x86_64-unknown-linux-musl/debug/query_runner /
USER 1000
CMD ["/query_runner"]

# Build Stage - used to build ONLY the query_runner image
FROM rust:1.76.0 AS release_builder
WORKDIR /usr/src/

RUN apt-get update 
RUN apt-get install musl musl-tools python3-pip -y 
RUN pip3 install cargo-zigbuild --break-system-packages
RUN rustup target add x86_64-unknown-linux-musl

COPY ./ ./
RUN cargo zigbuild --target x86_64-unknown-linux-musl -p query_runner --release

#Bundle Stage if using solo build of query_runner
FROM scratch as release_rebuild
COPY --from=release_builder /usr/src/target/x86_64-unknown-linux-musl/release/query_runner /
USER 1000
CMD ["/query_runner"]


#References the base build image, which contains prebuild binaries of all system components
FROM debug_build as debug_build

#Copy the prebuild binary from the base build image
FROM scratch as debug_prebuild
COPY --from=debug_build /query_runner /
USER 1000
CMD ["/query_runner"]

#References the base build image, which contains prebuild binaries of all system components
FROM release_build as release_build

#Copy the prebuild binary from the base build image
FROM scratch as release_prebuild
COPY --from=release_build /query_runner /
USER 1000
CMD ["/query_runner"]