# syntax=docker/dockerfile:1
FROM rust:1-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="--remap-path-prefix=/app=/src --remap-path-prefix=/usr/local/cargo=/cargo"
RUN cargo build --locked --release --bin channels-manager-v1 \
    && readelf --sections --wide target/release/channels-manager-v1 > /tmp/binary-sections \
    && if grep -Eq '\.(symtab|debug_[[:alnum:]_]+)[[:space:]]' /tmp/binary-sections; then \
         echo "Release binary contains symbols or debug information"; exit 1; \
       fi

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:54df941ed0d06a1bd95ef5e0ce391fd8d9f94b64782dc9a60062727849ee3f97 AS runtime
WORKDIR /app
COPY --from=build --chmod=0555 /app/target/release/channels-manager-v1 /app/channels-manager-v1
USER 65532:65532
STOPSIGNAL SIGTERM
ENTRYPOINT ["/app/channels-manager-v1"]
