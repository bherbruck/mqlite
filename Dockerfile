# Local development build
FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release
RUN cp target/release/mqlite /mqlite

# Final minimal image
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /mqlite /mqlite
COPY mqlite.toml /etc/mqlite/mqlite.toml

# TLS certificates should be mounted at runtime:
#   -v /path/to/certs:/etc/mqlite/certs:ro
# Then configure in mqlite.toml:
#   [tls]
#   enabled = true
#   cert = "/etc/mqlite/certs/cert.pem"
#   key = "/etc/mqlite/certs/key.pem"
VOLUME /etc/mqlite/certs

# Data directory for persistence
VOLUME /var/lib/mqlite

EXPOSE 1883 8883 9090
ENTRYPOINT ["/mqlite", "-c", "/etc/mqlite/mqlite.toml"]
