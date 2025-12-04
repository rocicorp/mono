FROM golang:1.23 AS litestream

WORKDIR /src/
RUN git clone --depth 1 --branch zero@v0.0.8 https://github.com/rocicorp/litestream.git
WORKDIR /src/litestream/

ARG LITESTREAM_VERSION=0.3.13+z0.0.8  # upstream version + zero version

RUN --mount=type=cache,target=/root/.cache/go-build \
	--mount=type=cache,target=/go/pkg \
	go build -ldflags "-s -w -X 'main.Version=${LITESTREAM_VERSION}' -extldflags '-static'" -tags osusergo,netgo,sqlite_omit_load_extension -o /usr/local/bin/litestream ./cmd/litestream

FROM node:22.15.0-alpine3.20

ARG ZERO_VERSION
ARG ZERO_SYNC_PROTOCOL_VERSION
ARG ZERO_MIN_SUPPORTED_SYNC_PROTOCOL_VERSION

RUN test -n "$ZERO_VERSION"
RUN test -n "$ZERO_SYNC_PROTOCOL_VERSION"
RUN test -n "$ZERO_MIN_SUPPORTED_SYNC_PROTOCOL_VERSION"

# Surface information about the build or server for the purposes of
# production / release management.
LABEL zero.version="$ZERO_VERSION"
LABEL zero.sync-protocol-version="$ZERO_SYNC_PROTOCOL_VERSION"
LABEL zero.min-supported-sync-protocol-version="$ZERO_MIN_SUPPORTED_SYNC_PROTOCOL_VERSION"

RUN apk add --update curl
RUN apk add --no-cache readline readline-dev

RUN mkdir -p /opt/app
WORKDIR /opt/app

COPY pkgs/ /tmp/pkgs/

# Install zero package based on version format
RUN if echo "$ZERO_VERSION" | grep -q "\.tgz$"; then \
    FILENAME=$(basename $ZERO_VERSION) && \
    echo "Installing from local tarball $FILENAME" && \
    cp /tmp/pkgs/$FILENAME . && \
    npm install -g ./$FILENAME && \
    rm -rf /tmp/pkgs; \
  else \
    echo "Installing from npm @rocicorp/zero@$ZERO_VERSION" && \
    npm install -g @rocicorp/zero@${ZERO_VERSION}; \
  fi

# Add zero-sqlite3 to path
RUN ln -s /usr/local/lib/node_modules/@rocicorp/zero/node_modules/@rocicorp/zero-sqlite3/build/Release/zero_sqlite3  /usr/local/bin/zero-sqlite3

# Copy litestream executable and config.yml
COPY --from=litestream /usr/local/bin/litestream /usr/local/bin/litestream
RUN cp /usr/local/lib/node_modules/@rocicorp/zero/out/zero-cache/src/services/litestream/config.yml /etc/litestream.yml

ENV ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream
ENV ZERO_LITESTREAM_CONFIG_PATH=/etc/litestream.yml
ENV ZERO_LOG_FORMAT=json
ENV ZERO_SERVER_VERSION=${ZERO_VERSION}
ENV ZERO_IN_CONTAINER=1

EXPOSE 4848 4849
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["npx zero-cache"]
