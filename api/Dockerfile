FROM rust:1.89 AS build

WORKDIR /source
COPY Cargo.toml .
COPY Cargo.lock .
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release

COPY .sqlx .sqlx
COPY src src
RUN touch src/main.rs
RUN cargo build --release

FROM debian:13-slim
WORKDIR /app
COPY --from=build /source/target/release/rinha-vimsos .
ENTRYPOINT ["./rinha-vimsos"]