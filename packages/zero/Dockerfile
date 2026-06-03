FROM golang:1.25.10@sha256:cd05a378aaf011e8056745363e5c40f4f2bef0fa4d9bf19b9c38316079c332ff AS litestream

WORKDIR /src/
RUN git clone --depth 1 --branch zero@v0.0.9 https://github.com/rocicorp/litestream.git
WORKDIR /src/litestream/

ARG LITESTREAM_VERSION=0.3.13+z0.0.9  # upstream version + zero version

# Force use of the toolchain bundled in the builder image so the resulting
# binary is compiled with Go 1.25.10 stdlib (fixes CVE-2025-68121 and a
# cluster of HIGH stdlib CVEs that affect any older toolchain pinned by
# the cloned go.mod's `toolchain` directive).
ENV GOTOOLCHAIN=local

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

FROM litestream/litestream:0.5.11 AS litestream-v5

FROM node:22.22.3-alpine3.23@sha256:968df39aedcea65eeb078fb336ed7191baf48f972b4479711397108be0966920

ARG ZERO_VERSION

RUN test -n "$ZERO_VERSION"

RUN apk add --update curl
RUN apk add --no-cache readline readline-dev
# @rocicorp/zero-sqlite3 >= 1.1.0 dynamically links ICU on Linux (musl static
# ICU is not -fPIC), so the runtime image must provide libicu or the native
# module fails to load. icu-libs matches the ICU major the prebuilt binary was
# built against (Alpine 3.x ships ICU 76).
RUN apk add --no-cache icu-libs

# Install pnpm via corepack (bundled with Node). Matches the monorepo's
# packageManager field. pnpm's build-script allowlist (v10+) blocks lifecycle
# scripts from transitive deps unless explicitly opted in, shrinking the
# postinstall supply-chain surface to packages we name below.
RUN corepack enable && corepack prepare pnpm@11 --activate

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
# it allowlisted unless runtime testing confirms tsx is never invoked.
RUN cat > pnpm-workspace.yaml <<'EOF'
allowBuilds:
  '@rocicorp/zero-sqlite3': true
  esbuild: true
EOF

COPY pkgs/ /tmp/pkgs/

# Install zero package based on version format
RUN if echo "$ZERO_VERSION" | grep -q "\.tgz$"; then \
    FILENAME=$(basename $ZERO_VERSION) && \
    echo "Installing from local tarball $FILENAME" && \
    cp /tmp/pkgs/$FILENAME . && \
    pnpm add ./$FILENAME && \
    rm -rf /tmp/pkgs ./$FILENAME; \
  else \
    echo "Installing from npm @rocicorp/zero@$ZERO_VERSION" && \
    pnpm add @rocicorp/zero@${ZERO_VERSION}; \
  fi

# Put zero's bin shims on PATH so `zero-cache` resolves without npx.
ENV PATH="/opt/app/node_modules/.bin:${PATH}"

# Copy litestream executable and config.yml
COPY --from=litestream /usr/local/bin/litestream /usr/local/bin/litestream
COPY --from=litestream-v5 /usr/local/bin/litestream /usr/local/bin/litestream-v5
RUN cp /opt/app/node_modules/@rocicorp/zero/out/zero-cache/src/services/litestream/config.yml /etc/litestream.yml

ENV ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream
ENV ZERO_LITESTREAM_EXECUTABLE_V5=/usr/local/bin/litestream-v5
ENV ZERO_LITESTREAM_CONFIG_PATH=/etc/litestream.yml
ENV ZERO_LOG_FORMAT=json
ENV ZERO_SERVER_VERSION=${ZERO_VERSION}
ENV ZERO_IN_CONTAINER=1

EXPOSE 4848 4849
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["zero-cache"]
