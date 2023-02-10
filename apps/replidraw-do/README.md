# Replidraw

A tiny Figma-like multiplayer graphics editor.

Built with [Replicache](https://replicache.dev), [Next.js](https://nextjs.org/),
and [Cloudflare Workers](https://workers.cloudflare.com/).

Running live at [replidraw-do.vercel.app](https://replidraw-do.vercel.app/).

## Hacking Locally

The `dev-worker` command runs the worker using [wrangler](https://developers.cloudflare.com/workers/wrangler/).

```bash
npm install

# Generate a secure shared secret enabling Reflect Server to
# authenticate calls from the front-end, e.g. to create a new
# room. Configure Reflect Server with the key via wrangler:
npx wrangler secret put REFLECT_AUTH_API_KEY

# OPTIONAL: if you want logs and metrics reported to Datadog, set
# this secret to a Datadog API key.
npx wrangler secret put REFLECT_DATADOG_API_KEY

# start the backend (Reflect Server)
npm run dev-worker

# (in another shell) start the frontend
REFLECT_API_KEY=<share secret from above> \
NEXT_PUBLIC_WORKER_HOST=ws://127.0.0.1:8787 \
npm run dev
```

## Publishing Worker to Cloudflare

First, get an account at Cloudflare: https://workers.cloudflare.com/.

Then:

```bash
# Set the secrets if you have not already:
npx wrangler secret put REFLECT_AUTH_API_KEY
npx wrangler secret put REFLECT_DATADOG_API_KEY # Optional

# publish to Cloudflare
npx wrangler publish

# run frontend
REFLECT_API_KEY=<shared secret> \
NEXT_PUBLIC_WORKER_HOST=wss://<host from previous command> \
npm run dev
```

## More information

More information, including [how auth/autz works](https://github.com/rocicorp/reflect-todo#authentication-and-authorization) and the [Reflect Server API](https://github.com/rocicorp/reflect-todo#server-api) see the docs in [reflect-todo](https://github.com/rocicorp/reflect-todo). Essentially all the documentation there applies to `replidraw-do` as well.

### Building your Own Thing

1. Clone this project
2. `npm install`
3. Adjust the mutators, subscribers, and entities in `datamodel/` to implement your domain objects.
4. Implement your UI by calling the subscribers and mutators. See existing UI in `frontend` for an example.
5. Have fun 😀.

### How to persist logs from the worker

The `BaseServer` class accepts a `logger` argument. You can implement this yourself to send the logs wherever you want. We also provide a `DatadogLogger` implementation of this interface in the package as a convenience, if you have an account there. [See worker/index.ts for an example](https://github.com/rocicorp/replidraw-do/blob/main/worker/index.ts#L17).
