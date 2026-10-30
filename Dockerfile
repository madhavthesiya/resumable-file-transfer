# Build once, run as either role. The same image is the server and both
# clients in docker-compose.yml, which is what makes the three-node demo
# reproducible on one machine.
#
#   docker build -t resumable-file-transfer .
#   docker compose up

# ---------------------------------------------------------------- build
FROM debian:bookworm-slim AS build

RUN apt-get update \
 && apt-get install -y --no-install-recommends gcc libc6-dev make python3 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY src/ src/
COPY tools/ tools/

# Each implementation builds into its own directory, exactly as it does from a
# git checkout; nothing here is special-cased for the container.
RUN set -eux; \
    for impl in baseline restart resume parallel; do \
        make -C "src/$impl"; \
    done

# The demo corpus is generated rather than copied: it is deterministic, so a
# rebuild produces byte-identical files, and the image carries no dataset.
RUN python3 tools/make_dataset.py --out /dataset --demo

# ---------------------------------------------------------------- runtime
FROM debian:bookworm-slim

# iproute2 gives the entrypoint `ss` to wait for the server's port; coreutils
# is already present. Nothing else is needed at runtime - the binaries are
# statically linked against nothing but libc and libpthread.
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --create-home --shell /bin/bash app

WORKDIR /app
COPY --from=build /build/src/baseline/server  bin/baseline-server
COPY --from=build /build/src/baseline/client  bin/baseline-client
COPY --from=build /build/src/restart/server   bin/restart-server
COPY --from=build /build/src/restart/client   bin/restart-client
COPY --from=build /build/src/resume/server    bin/resume-server
COPY --from=build /build/src/resume/client    bin/resume-client
COPY --from=build /build/src/parallel/server  bin/parallel-server
COPY --from=build /build/src/parallel/client  bin/parallel-client
COPY --from=build /dataset /dataset
COPY docker/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh /app/bin/* && chown -R app:app /app /dataset
USER app

ENV PATH="/app/bin:${PATH}" \
    DATASET=/dataset/demo \
    PORT=34000

EXPOSE 34000-34030
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["server"]
