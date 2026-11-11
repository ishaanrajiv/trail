# Builds the `trail` binary and puts it in a small image.
#   docker build -t trail .
#   docker run -p 127.0.0.1:8200:8200 -v trail-data:/data trail
FROM rust:1-slim-bookworm AS build
WORKDIR /src
# Keep the build off every core: it compiles SQLite and zstd from C.
ENV CARGO_BUILD_JOBS=4
COPY . .
RUN cargo build --release --locked -p trail

FROM debian:bookworm-slim
# tzdata: Trail reads the system's timezone database for `timezone` in trail.toml.
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /data --shell /usr/sbin/nologin trail && mkdir /data && chown trail /data \
    && printf 'listen = "0.0.0.0:8200"\ndata_dir = "/data"\n' > /etc/trail.toml
COPY --from=build /src/target/release/trail /usr/local/bin/trail
USER trail
VOLUME /data
EXPOSE 8200
# Inside a container the port is published by Docker, so listening on every interface is right
# here. Publish it to loopback (as above) unless something in front of it checks who is calling.
CMD ["trail", "serve", "--config", "/etc/trail.toml"]
