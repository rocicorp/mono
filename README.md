# Replicache JS SDK

![Node.js CI](https://github.com/rocicorp/replicache-sdk-js/workflows/Node.js%20CI/badge.svg)

## Development Instructions

At the moment we haven't published this to npm. Until then you can add a `file:` or as a github dependency.

### Get the Code

Either by `npm install`'ing the git repo, cloning it, or downloading a
release.

### Build the JS

Replicache JS SDK is written in TypeScript. Run `npm run build` to generate the JS source code (JS source is outputted in `out/`). By default we generate browser compatible JS but you can also build CommonJS modules by running `npm run build:cjs`. Let us know what your needs are.

### Get Binaries

The binaries are downloaded when you do `npm install`. If for some reason you need to redownload these you can manully run `tool/build.sh`. Do this again whenever you update the SDK.

### Run `test-server`

Currently, the JavaScript SDK relies on a native local server that
implements the guts of the sync protocol on the client side. This is
temporary and will be removed.

For now, you must have this server running whenever you are working
with the SDK:

```
mkdir ~/.repm
npx test-server --storage-dir=$HOME/.repm
```

### Start your Data Layer

See [Replicache Server Setup](https://github.com/rocicorp/replicache#server-side) for server-side instructions.

For the rest of these instructions we will assume your data layer is
running on `localhost:3000`.

### Start Diff-Server

In production, your app will talk to the production Replicache diff-server at https://serve.replicache.dev/.

During development, that server can't reach your workstation, so we
provide a development instance to work against instead. Leave this
running in a tab:

```bash
# The --client-view flag should point to the Client View endpoint
# on your development data layer.
npx diff-server --client-view="http://localhost:3000/replicache-client-view"
```

### Including the JS

It is recommended to use ES modules (but we also include CommonJS for backwards compat).

```js
import Replicache, {REPMHTTPInvoker} from 'replicache';
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
const value = await replicache.query(tx => tx.get('/hello'));
console.log(value);
```
