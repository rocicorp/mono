FROM --platform=linux/amd64 node:18.20.4-alpine3.20
ARG NPM_TOKEN
RUN mkdir -p /opt/app
WORKDIR /opt/app
COPY . ./
RUN echo "//registry.npmjs.org/:_authToken=${NPM_TOKEN}" > .npmrc && \
    npm run build-ci && \
    rm -f .npmrc
RUN apk add --update curl
EXPOSE 3000
CMD [ "npm", "run", "start-zero-cache"]