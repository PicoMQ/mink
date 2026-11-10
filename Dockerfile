# syntax=docker/dockerfile:1

FROM node:24-bookworm-slim AS console
WORKDIR /src/console

COPY console/package.json console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY console ./
RUN npm run check && npm run lint && npm test && npm run build

FROM rust:1-bookworm AS build
WORKDIR /src

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
 && rm -rf /var/lib/apt/lists/*

ARG PROFILE=release
COPY . .
COPY --from=console /src/mink/mink-server/console /src/mink/mink-server/console
RUN --mount=type=cache,id=mink-target,sharing=locked,target=/src/target \
    --mount=type=cache,id=mink-cargo-registry,sharing=locked,target=/usr/local/cargo/registry \
    --mount=type=cache,id=mink-cargo-git,sharing=locked,target=/usr/local/cargo/git \
    cargo build --locked --profile "$PROFILE" -p mink-cli \
 && mkdir -p /out \
 && cp "/src/target/$( [ "$PROFILE" = dev ] && echo debug || echo "$PROFILE" )/mink" /out/mink

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 mink \
 && useradd --system --uid 10001 --gid mink --no-create-home mink \
 && install -d -o mink -g mink /cache

COPY --from=build /out/mink /usr/local/bin/mink

USER mink
VOLUME ["/cache"]
ENTRYPOINT ["mink"]
CMD ["serve"]
