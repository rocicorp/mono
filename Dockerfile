FROM --platform=linux/amd64 node:18.20.4-alpine3.20
RUN apk add g++ make py3-pip
ARG NPM_TOKEN
RUN mkdir -p /opt/app
RUN mkdir -p /data/db
WORKDIR /opt/app
COPY . ./
COPY --from=litestream/litestream:latest /usr/local/bin/litestream /usr/local/bin/litestream
RUN echo "//registry.npmjs.org/:_authToken=${NPM_TOKEN}" > .npmrc && \
    npm run build-ci && \
    rm -f .npmrc
RUN apk add --update curl
WORKDIR /opt/app/packages/zero-cache
EXPOSE 3000
RUN ./restore-litestream-db.sh
CMD ["litestream", "replicate", "-config", "/opt/app/litestream.yml"]