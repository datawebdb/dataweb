# Build Stage - used to build ONLY the flight_server image
FROM rust:1.76.0-bullseye AS debug_builder
WORKDIR /usr/src/

COPY ./ ./
RUN cargo build -p flight_server

#Bundle Stage if using solo build of flight_server
FROM ubuntu:22.04 as debug_rebuild
COPY --from=debug_builder /usr/src/target/debug/flight_server /
USER 1000
RUN apt-get update && apt-get install libpq-dev -y
CMD ["/flight_server"]

# Build Stage - used to build ONLY the flight_server image
FROM rust:1.76.0-bullseye AS release_builder
WORKDIR /usr/src/

COPY ./ ./
RUN cargo build -p flight_server --release

#Bundle Stage if using solo build of flight_server
FROM debian:11 as release_rebuild
COPY --from=release_builder /usr/src/target/release/flight_server /
RUN apt-get update && apt-get install libpq-dev -y
USER 1000
CMD ["/flight_server"]


#References the base build image, which contains prebuild binaries of all system components
FROM debug_build as debug_build

#Copy the prebuild binary from the base build image
FROM ubuntu:22.04 as debug_prebuild
COPY --from=debug_build /flight_server /
RUN apt-get update && apt-get install libpq-dev -y
USER 1000
CMD ["/flight_server"]

#References the base build image, which contains prebuild binaries of all system components
FROM release_build as release_build

#Copy the prebuild binary from the base build image
FROM debian:11 as release_prebuild
COPY --from=release_build /flight_server /
RUN apt-get update && apt-get install libpq-dev -y
USER 1000
CMD ["/flight_server"]