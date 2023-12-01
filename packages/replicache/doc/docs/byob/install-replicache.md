---
title: Setup
slug: /byob/setup
---

Replicache is framework agnostic, and you can use most any libraries and frameworks you like.

We're going to use [Next.js](https://nextjs.org/) for this sample app, just because it's a convenient way to build a monorepo JavaScript-based web app.

Create an empty Next.js project:

```bash
npx create-next-app --example=basic-css chat
cd chat
```

Install Replicache and a few other utilities we'll use for this sample:

```bash
npm install replicache replicache-react nanoid pg pg-mem pg-promise pusher pusher-js
```

## Next

Next, we'll [design our Client View](./design-client-view.md) – the schema for our client-side data.
