#References the base build image, which contains prebuild binaries of all system components
FROM debug_build as debug_build

#Copy the prebuild binary from the base build image
FROM scratch as debug_prebuild
COPY --from=debug_build /relayctl /
USER 1000
ENTRYPOINT ["/relayctl"]

#References the base build image, which contains prebuild binaries of all system components
FROM release_build as release_build

#Copy the prebuild binary from the base build image
FROM scratch as release_prebuild
COPY --from=release_build /relayctl /
USER 1000
ENTRYPOINT ["/relayctl"]