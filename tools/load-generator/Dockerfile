FROM node:20-alpine
WORKDIR /app
COPY package.json /app/package.json
COPY packages/shared /app/packages/shared
COPY tools/load-generator /app/tools/load-generator
RUN npm install
WORKDIR /app/tools/load-generator
ENTRYPOINT ["npm", "run", "go", "--"]
