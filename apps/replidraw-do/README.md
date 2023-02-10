# Replidraw

A tiny Figma-like multiplayer graphics editor.

Built with [Replicache](https://replicache.dev), [Next.js](https://nextjs.org/),
and [Cloudflare Workers](https://workers.cloudflare.com/).

Running live at [replidraw-do.vercel.app](https://replidraw-do.vercel.app/).

## Hacking Locally

The `dev-worker` command runs the worker using [Miniflare](https://miniflare.dev/), a really nice Cloudflare emulation environment. This is super convenient and doesn't require a CF account. Unfortunately, it also doesn't give you a realistic view of performance since everything is local.

```bash
npm install

# start the backend
npm run dev-worker

# (in another shell) start the frontend
NEXT_PUBLIC_WORKER_HOST=ws://localhost:8787 npm run dev
```

## Publishing Worker to Cloudflare

1. Get an account at Cloudflare: https://workers.cloudflare.com/.
2. Install the [Wrangler CLI tool](https://developers.cloudflare.com/workers/cli-wrangler/install-update)

Then:

```
# publish to Cloudflare
wrangler publish

# run frontend
npm run dev
```

## Developing against Cloudflare

It is possible to develop using the Cloudflare network without destabilizing the production copy of a worker. This provides a more realistic idea of latency than Miniflare.

```
# Must have published at least once prior
wrangler dev

# (In another shell) Run frontend
npm run dev
```

## Using Reflect in your own project

1. Copy `reflect-*.tgz`, `reflect-client-*.tgz`, `worker`, and `wrangler.toml` into your project
2. `npm add reflect-*.tgz reflect-client-*.tgz @cloudflare/workers-types esbuild miniflare`
3. Adjust the `mutators` param in `worker/index.ts` to point to your mutators file.
4. Copy the `dev-worker` and `build-worker` scripts from `package.json` into your project
5. Ensure `Replicache` and the client are instantiated correctly, see `[id].tsx` in this project.
6. Have fun 😀.

Note: This will get easier. `reflect-*.tgz` will soon become a standalone npm module, and `reflect-client-*.tgz` will get folded into the existing `replicache` module.

## Recipes

### How to persist logs from the worker

The `BaseServer` class accepts a `logger` argument. You can implement this yourself to send the logs wherever you want. We also provide a `DatadogLogger` implementation of this interface in the package as a convenience, if you have an account there. [See worker/index.ts for an example](https://github.com/rocicorp/replidraw-do/blob/main/worker/index.ts#L17).

### How to list the rooms for your Reps server

Cloudflare doesn't have a UI for this, but there's an API.

First, go to https://dash.cloudflare.com/profile/api-tokens and click "Create Token" then choose the "Read All Resources" template. Click through and then copy the resulting token.

```bash
# Get the account id
curl -X GET "https://api.cloudflare.com/client/v4/accounts" \
     -H "Authorization: Bearer :token" \
     -H "Content-Type:application/json"

# Get namespace for account
curl -X GET "https://api.cloudflare.com/client/v4/accounts/:accountid/workers/durable_objects/namespaces" \
     -H "Authorization: Bearer :token" \
     -H "Content-Type:application/json"

# Get object instances
curl -X GET "https://api.cloudflare.com/client/v4/accounts/:accountid/workers/durable_objects/namespaces/:namespaceid/objects" \
     -H "Authorization: Bearer :token" \
     -H "Content-Type:application/json"
```
