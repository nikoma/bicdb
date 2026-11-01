# syntax=docker/dockerfile:1
FROM rust:1.96-bookworm AS engine
RUN apt-get update && apt-get install -y --no-install-recommends \
    clang cmake libclang-dev libssl-dev pkg-config protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY . .
ENV CARGO_BUILD_JOBS=2 CARGO_PROFILE_RELEASE_DEBUG=0
RUN --mount=type=cache,id=bicdb-rails-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=bicdb-rails-target,target=/source/target \
    cargo build --locked --release -p bicdb-cli --bin bicdb \
    && cp target/release/bicdb /bicdb \
    && strip --strip-debug /bicdb

FROM ruby:3.4-slim@sha256:3a8122c4267d89a95b1ce0f3029b44e325180314661856300d29d3e8a80ec46f AS rails_dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev libssl-dev libyaml-dev git \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY examples/rails/team_board/Gemfile examples/rails/team_board/Gemfile.lock ./
RUN bundle install --jobs 4

FROM rails_dependencies AS demo
COPY --from=engine /bicdb /usr/local/bin/bicdb
COPY LICENSE LICENSE-SCOPE.md NOTICE THIRD_PARTY_NOTICES.md /usr/share/bicdb/
COPY LICENSES /usr/share/bicdb/LICENSES
COPY examples/rails/team_board/ ./
ENV BICDB_BIN=/usr/local/bin/bicdb BICDB_PORT=5433 BINDING=0.0.0.0 PORT=3000
EXPOSE 3000
CMD ["bin/dev"]
