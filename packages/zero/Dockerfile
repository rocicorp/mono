FROM --platform=linux/amd64 node:22.11.0-alpine3.20

ARG ZERO_VERSION
RUN test -n "$ZERO_VERSION"
ENV ZERO_VERSION=$ZERO_VERSION

RUN mkdir -p /opt/app
WORKDIR /opt/app
RUN npm login
RUN npm install -g @rocicorp/zero@${$ZERO_VERSION}
EXPOSE 4848
ENTRYPOINT ["/bin/sh", "-c"]
ENV ZERO_LOG_LEVEL=debug
ENV ZERO_LOG_FORMAT=json
CMD ["npx zero-cache"]
