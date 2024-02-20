#References the base build image, which contains prebuild binaries of all system components
FROM debug_build as debug_build

#Copy the prebuild binary from the base build image
FROM ubuntu:22.04 as debug_prebuild
COPY --from=debug_build /data_web_engine /
USER 1000
ENTRYPOINT ["/data_web_engine"]

#References the base build image, which contains prebuild binaries of all system components
FROM release_build as release_build

#Copy the prebuild binary from the base build image
FROM debian:11 as release_prebuild
COPY --from=release_build /data_web_engine /
USER 1000
ENTRYPOINT ["/data_web_engine"]