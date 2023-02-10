---
title: Hello, Replicache
slug: /
---

The easiest way to get started is with our Todo starter app.

This app is a simple, but fully-functional Replicache app built with React. The backend is provided by [replicache-express](https://github.com/rocicorp/replicache-express) — a generic Express Replicache server.

This app is a good way to play with Replicache, but it is _also_ a great foundation on which to build your own app using Replicache. Since it is small and has all the pieces you'll need, you can clone it and then build your own app from there.

# Prerequisites

You'll need [Node.js](https://nodejs.dev/) v16 or greater.

# Install

```bash
npx create-replicache-app my-app react
cd my-app
npm install

# Get a Replicache license key. The command below will ask you a few quick
# questions and then print out your key.
npx replicache get-license

export VITE_REPLICACHE_LICENSE_KEY="<your license key>"

npm run watch --ws
```

# 🎉 Tada!

You now have a simple todo app powered by Replicache and Express.

<p class="text--center">
  <img src="/img/setup/todo.webp" width="650"/>
</p>

Open the app in a browser window, copy the resulting url, and open a second browser window to it. With the two windows side-by-side, add some items in one window and see them reflected in the other. Woo!

:::note

By default the dev backend stores data in memory, so if you restart the server, the data is lost. See [Local Postgres](/local-postgres) for setting up local persistence.

:::

# Next

The [next section](/app-features) reiterates some of the key features of this little demo app that Replicache enables.
