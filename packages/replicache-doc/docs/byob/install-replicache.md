---
title: Setup
slug: /byob/setup
---

Replicache is framework agnostic, and you can use most any libraries and frameworks you like.

In this guide, we're going to use Express/Vite/React. To start, clone the [BYOB starter](https://github.com/rocicorp/byob-starter) repo:

```bash
git clone git@github.com:rocicorp/byob-starter.git
cd byob-starter
npm install
```

This project is a monorepo web app with three workspaces: `client`, `server`, and `shared`. The `client` workspace contains the client-side UI, developed with [Vite](https://vitejs.dev/) and [React](https://react.dev/). The `server` workspace contains the server-side logic, implemented using [Express](https://expressjs.com/). And the `shared` workspace contains types and classes that are shared between client and server.

## Next

Next, we'll [design our Client View](./design-client-view.md) – the schema for our client-side data.
