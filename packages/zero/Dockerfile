FROM litestream/litestream:0.3.13 as litestream
FROM node:22.11.0-alpine3.20

ARG ZERO_VERSION

RUN test -n "$ZERO_VERSION"

RUN apk add --update curl

RUN mkdir -p /opt/app
WORKDIR /opt/app

# Copy the entire prod/zbugs directory from the build context
COPY packages/zero/pkgs/ /tmp/pkgs/

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

# Copy litestream executable and config.yml
COPY --from=litestream /usr/local/bin/litestream /usr/local/bin/litestream
RUN cp /usr/local/lib/node_modules/@rocicorp/zero/out/zero-cache/src/services/litestream/config.yml /etc/litestream.yml

ENV ZERO_LITESTREAM_EXECUTABLE=/usr/local/bin/litestream
ENV ZERO_LITESTREAM_CONFIG_PATH=/etc/litestream.yml
ENV ZERO_LOG_LEVEL=debug
ENV ZERO_LOG_FORMAT=json

EXPOSE 4848 4849
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["npx zero-cache"]
