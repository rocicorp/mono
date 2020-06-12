# Replicache JS SDK

![Node.js CI](https://github.com/rocicorp/replicache-sdk-js/workflows/Node.js%20CI/badge.svg)

## Getting Started

Eventually you will be able to `npm install` but until then...

## Prerequisites

- NodeJS/NPM

## Building the code

### Build the JS

Replicache JS SDK is written in TypeScript. Run `npm run build` to generate the JS source code (JS source is outputted in `out/`). By default we generate browser compatible JS but you can also build CommonJS modules by running `npm run build:cjs`. Let us know what your needs are.

### Get `test-server`

The Replicache JS SDK currently relies on a native local binary, 
`test-server`. This is a temporary limitation and will be fixed. Fetch 
the corect version of the binary with: `npm run build:test-server`. 
Re-run this whenever you update the SDK code.

## Run `test-server`

You'll need to leave this running in a tab while you're using the SDK:

```sh
npm run start:test-server
```

This starts an HTTP server on port 7002.

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
