FROM node:22.11.0-alpine3.20

ARG ZERO_VERSION
ARG NPM_TOKEN

RUN test -n "$ZERO_VERSION"
RUN test -n "$NPM_TOKEN"

RUN mkdir -p /root/.npm && echo "${NPM_TOKEN}" > /root/.npmrc

RUN mkdir -p /opt/app
WORKDIR /opt/app
RUN npm install -g @rocicorp/zero@${ZERO_VERSION}
EXPOSE 4848
ENTRYPOINT ["/bin/sh", "-c"]
ENV ZERO_LOG_LEVEL=debug
ENV ZERO_LOG_FORMAT=json
CMD ["npx zero-cache"]
