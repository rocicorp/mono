FROM golang:1.25.10@sha256:cd05a378aaf011e8056745363e5c40f4f2bef0fa4d9bf19b9c38316079c332ff AS litestream

WORKDIR /src/
RUN git clone --depth 1 --branch zero@v0.0.10 https://github.com/rocicorp/litestream.git
WORKDIR /src/litestream/

ARG LITESTREAM_VERSION=0.3.13+z0.0.10  # upstream version + zero version

# Force use of the toolchain bundled in the builder image so the resulting
# binary is compiled with Go 1.25.10 stdlib (fixes CVE-2025-68121 and a
# cluster of HIGH stdlib CVEs that affect any older toolchain pinned by
# the cloned go.mod's `toolchain` directive).
ENV GOTOOLCHAIN=local

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

# TODO: Use the official litestream image once our fork is no longer necessary, i.e.
# FROM litestream/litestream:0.5.16 AS litestream-v5
FROM golang:1.25.10@sha256:cd05a378aaf011e8056745363e5c40f4f2bef0fa4d9bf19b9c38316079c332ff AS litestream-v5

# Install build dependencies for VFS extension
RUN apt-get update && apt-get install -y gcc libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /src/
RUN git clone --depth 1 --branch zero-v5@v0.5.16+z0.0.1 https://github.com/rocicorp/litestream.git
WORKDIR /src/litestream
COPY . .

ARG LITESTREAM_V5_VERSION=0.5.16+z0.0.1 # upstream version + zero version 

# Build litestream binary
RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_V5_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

# Build VFS loadable extension
RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	mkdir -p dist && \
	CGO_ENABLED=1 go build \
	-tags "vfs,SQLITE3VFS_LOADABLE_EXT" \
	-buildmode=c-archive \
	-o dist/litestream-vfs.a ./cmd/litestream-vfs && \
	mv dist/litestream-vfs.h src/litestream-vfs.h && \
	gcc -DSQLITE3VFS_LOADABLE_EXT -g -fPIC -shared \
	-o dist/litestream-vfs.so \
	src/litestream-vfs.c \
	dist/litestream-vfs.a \
	-lpthread -ldl -lm

RUN mv /src/litestream/dist/litestream-vfs.so /usr/local/lib/litestream-vfs.so

FROM node:22.23.1-alpine3.23@sha256:8516dce0483394d5708d4b2ee6cacb79fb1d617ea4e2787c2120bcca92ce372e

ARG ZERO_VERSION
ARG ZERO_PACKAGE

RUN test -n "$ZERO_VERSION"

RUN apk add --update curl
RUN apk add --no-cache readline readline-dev

# Install pnpm via corepack (bundled with Node). Matches the monorepo's
# packageManager field. pnpm's build-script allowlist (v10+) blocks lifecycle
# scripts from transitive deps unless explicitly opted in, shrinking the
# postinstall supply-chain surface to packages we name below.
RUN corepack enable && corepack prepare pnpm@11.11.0 --activate

RUN mkdir -p /opt/app
WORKDIR /opt/app

RUN cat > package.json <<'EOF'
{
  "name": "zero-runtime",
  "private": true
}
EOF

# Allowlist for lifecycle scripts, in pnpm-workspace.yaml because pnpm 11 no
# longer reads the `pnpm` field from package.json. zero-sqlite3 ships no native
# binary in its tarball (see its package.json `files`) — its `install` script
# runs prebuild-install to fetch one, so it must be allowed. esbuild is pulled
# in transitively via tsx and downloads a platform binary in postinstall; keep
# it allowlisted unless runtime testing confirms tsx is never invoked. protobufjs
# is pulled in by OpenTelemetry's gRPC exporters and runs a postinstall script.
RUN cat > pnpm-workspace.yaml <<'EOF'
allowBuilds:
  '@rocicorp/zero-sqlite3': true
  esbuild: true
  protobufjs: true
patchedDependencies:
  postgres@3.4.7: patches/postgres@3.4.7.patch
EOF

COPY patches/ patches/
COPY pkgs/ /tmp/pkgs/

# Install zero package based on install source format (tarball or pnpm)
RUN ZERO_INSTALL_SOURCE="${ZERO_PACKAGE:-$ZERO_VERSION}" && \
  if echo "$ZERO_INSTALL_SOURCE" | grep -q "\.tgz$"; then \
    FILENAME=$(basename "$ZERO_INSTALL_SOURCE") && \
    echo "Installing from local tarball $FILENAME" && \
    cp "/tmp/pkgs/$FILENAME" . && \
    pnpm add ./$FILENAME && \
    rm -rf /tmp/pkgs ./$FILENAME; \
  else \
    echo "Installing from npm @rocicorp/zero@$ZERO_INSTALL_SOURCE" && \
    pnpm add @rocicorp/zero@${ZERO_INSTALL_SOURCE}; \
  fi

# Put zero's bin shims on PATH so `zero-cache` resolves without npx.
ENV PATH="/opt/app/node_modules/.bin:${PATH}"

# Copy litestream executable and config.yml
COPY --from=litestream /usr/local/bin/litestream /usr/local/bin/litestream
COPY --from=litestream-v5 /usr/local/bin/litestream /usr/local/bin/litestream-v5
COPY --from=litestream-v5 /usr/local/lib/litestream-vfs.so /usr/local/lib/litestream-vfs.so
RUN cp /opt/app/node_modules/@rocicorp/zero/out/zero-cache/src/services/litestream/config.yml /etc/litestream.yml

ENV ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream
ENV ZERO_LITESTREAM_EXECUTABLE_V5=/usr/local/bin/litestream-v5
# This is the default value, but set explicitly as documentation.
ENV ZERO_LITESTREAM_VFS_EXTENSION_PATH=/usr/local/lib/litestream-vfs.so
ENV ZERO_LITESTREAM_CONFIG_PATH=/etc/litestream.yml
ENV ZERO_LOG_FORMAT=json
ENV ZERO_SERVER_VERSION=${ZERO_VERSION}
ENV ZERO_IN_CONTAINER=1

EXPOSE 4848 4849
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["zero-cache"]
