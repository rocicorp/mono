FROM --platform=linux/amd64 node:18.20.4-alpine3.20
RUN apk add g++ make py3-pip
ARG NPM_TOKEN
RUN mkdir -p /opt/app
RUN mkdir -p /data/db
WORKDIR /opt/app
COPY . ./
ADD https://github.com/benbjohnson/litestream/releases/download/v0.3.13/litestream-v0.3.13-linux-amd64.tar.gz /tmp/litestream.tar.gz
RUN tar -C /usr/local/bin -xzf /tmp/litestream.tar.gz
RUN chmod +x /usr/local/bin/litestream
RUN echo "//registry.npmjs.org/:_authToken=${NPM_TOKEN}" > .npmrc && \
    npm run build-ci && \
    rm -f .npmrc
RUN apk add --update curl
WORKDIR /opt/app/packages/zero-cache
RUN chmod +x ./restore-litestream-db.sh
EXPOSE 3000
ENTRYPOINT ["/bin/sh", "-c"]
ENV LITESTREAM=1
CMD ["(./restore-litestream-db.sh || true) && litestream replicate -config /opt/app/litestream.yml"]
