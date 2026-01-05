# Local development build
FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release
RUN cp target/release/mqlite /mqlite

# Final minimal image
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /mqlite /mqlite
COPY mqlite.toml /etc/mqlite/mqlite.toml

# Create data directory for persistence
VOLUME /var/lib/mqlite

EXPOSE 1883 9090
ENTRYPOINT ["/mqlite", "-c", "/etc/mqlite/mqlite.toml"]
