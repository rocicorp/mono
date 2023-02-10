---
title: App Structure
slug: /app-structure
---

# A Quick Tour of the Starter App

There's really just a few key bits:

- **[`src/todo.ts`](https://github.com/rocicorp/replicache-todo/blob/main/src/todo.ts)** defines the todo entity. You will typically have one such file for each entity in your application.
- **[`src/mutators.ts`](https://github.com/rocicorp/replicache-todo/blob/main/src/mutators.ts)** defines the [Replicache mutators](/how-it-works#mutations) for the todo app. Replicache mutators run twice: first immediately and optimistically on the client, then later authoritatively on the server. Replicache [automatically reconciles](/how-it-works#the-big-picture) differences between the optimistic and authoritative runs continuously on all clients.
- **[`pages/index.tsx`](https://github.com/rocicorp/replicache-todo/blob/main/pages/index.tsx)** is the app's entrypoint. It generates a new random todo list ID and redirects to it.
- **[`pages/d/[id].tsx`](https://github.com/rocicorp/replicache-todo/blob/main/pages/d/[id].tsx)** is the handler for a todo list. It instantiates Replicache.
- **[`src/app.tsx`](https://github.com/rocicorp/replicache-todo/blob/main/src/app.tsx)** is the top-level component. It subscribes to all todos in Replicache and renders them. It also wires up event handlers from the app to Replicache mutators.
- The server side is implemented with [replicache-nextjs](https://www.npmjs.com/package/replicache-nextjs). This is a simple, generic Replicache server built on Next.js and Postgres. It is wired up in [`pages/api/replicache/[op].ts`](https://github.com/rocicorp/replicache-todo/blob/main/pages/api/replicache/[op].ts).

# Next

Now [let's add a simple feature](/first-replicache-feature) to this app.
