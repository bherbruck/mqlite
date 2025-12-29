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

# Create data directory for persistence
VOLUME /var/lib/mqlite

EXPOSE 1883 9001
ENTRYPOINT ["/mqlite"]
