# Replicache JS SDK

![Node.js CI](https://github.com/rocicorp/replicache-sdk-js/workflows/Node.js%20CI/badge.svg)

## Getting Started

Eventually you will be able to `npm install` but until then...

## Prerequisites

- NodeJS/NPM

## Building the code

### Get the Code

Download the [latest release](https://github.com/rocicorp/replicache-sdk-js/releases) from Github. It's important to get the typescript and binary from a release to ensure you have the correct version of the binary.

### Build the JS

Replicache JS SDK is written in TypeScript. Run `npm run build` to generate the JS source code (JS source is outputted in `out/`). By default we generate browser compatible JS but you can also build CommonJS modules by running `npm run build:cjs`. Let us know what your needs are.

## Run the binary

The Replicache Client binary was originally developed as a quick way to test the Replicache Client API. Eventually we will use a WASM version or have Electron/NodeJS start the binary automatically. But for now we need to manually start it.

```sh
./repm-{arch}-{platform} --no-fake-time
```

This starts an HTTP server on port 7002 (by default, use `--port` to change.)

We need to use the `--no-fake-time` flag for now or all the timestamps will be using a fake time.

# Including the JS

By default we only compile the TS to an ES module.

```js
import Replicache, {REPMHTTPInvoker} from './out/mod.js';
```

To use `Replicache` you currently have to tell it how to invoke the **Rep**licache Client API **M**odule (REPM).

```js
const diffServerURL = 'https://serve.replicache.dev/pull';
const diffServerAuth = '<your diff-server account ID>';
const batchURL = 'https://youservice.com/replicache-batch';
const dataLayerAuth = '<your data-layer auth token>';
const repmInvoker = new REPMHTTPInvoker('http://localhost:7002');
const repmInvoke = repmInvoker.invoke;
const replicache = new Replicache({
  diffServerURL,
  diffServerAuth,
  batchURL,
  dataLayerAuth,
  repmInvoke,
});
await replicache.query(async tx => {
  console.log(await tx.get('/hello'));
});
```
