# Replicache JS SDK

![Node.js CI](https://github.com/rocicorp/replicache-sdk-js/workflows/Node.js%20CI/badge.svg)

## Getting Started

Eventually you will be able to `npm install` but until then...

## Prerequisites

- NodeJS/NPM
- Go binary - https://golang.org/dl/

## Building the code

### Build the JS

Replicache JS SDK is written in TypeScript. Run `npm run build` to generate the JS source code (JS source is outputted in `out/`). By default we generate browser compatible JS but you can also build CommonJS modules by running `npm run build:cjs`. let us know what your needs are.

### Build the Replicache Client test server binary

Run `tool/build.sh` which downloads the source code for https://github.com/rocicorp/replicache-client and builds the `build/replicache-client/test_server` binary.

## Run the binary

The Replicache Client test server binary was developed as a quick way to test the Replicache Client API. Eventually we will use a WASM version or have Electron/NodeJS start the binary automatically. But for now we need to manually start it.

```sh
build/replicache-client/test_server --no-fake-time
```

This starts an HTTP server on port 7002 (by default, use `--port` to change.)

We need to use the `--no-fake-time` flag for now ot all the timestamps will be using a fake time.

# Including the JS

By default we only compile the TS to an ES module.

```js
import Replicache, {RepmHttpInvoker} from './out/mod.js';
```

To use `Replicache` you currently have to tell it how to invoke the **Rep**licache Client API **M**odule (REPM).

```js
const diffServerUrl = 'https://serve.replicache.dev/pull';
const repmInvoker = new RepmHttpInvoker('http://localhost:7002');
const repmInvoke = repmInvoker.invoke;
const replicache = new Replicache({
  diffServerUrl,
  repmInvoke,
});
await replicache.query(async tx => {
  console.log(await tx.get('/hello'));
});
```
