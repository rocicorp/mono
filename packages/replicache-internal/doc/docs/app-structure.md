---
title: App Structure
slug: /app-structure
---

# A Quick Tour of the Starter App

There's really just a few key bits:

- **[`replicache-nextjs/`](https://github.com/rocicorp/replicache-todo/blob/main/replicache-nextjs)** contains a simple, generic Replicache server built on Next.js and Postgres. You don't need to worry about this directory for now — you only need to modify it for more advanced cases. If you're curious, you can learn more at [How Replicache Works](how-it-works.md).
- **[`pages/d/[id].tsx`](https://github.com/rocicorp/replicache-todo/blob/main/pages/d/[id].tsx)** is the page the app runs on. Each unique ID is a separate todo list.
- **[`src/todo.ts`](https://github.com/rocicorp/replicache-todo/blob/main/src/todo.ts)** defines the todo entity and its operations. Most of this is generated using the [@rocicorp/rails](https://github.com/rocicorp/rails) helper library.
- **[`src/mutators.ts`](https://github.com/rocicorp/replicache-todo/blob/main/src/mutators.ts)** defines the [Replicache mutators](/how-it-works#mutations) for the todo app. Replicache mutators run twice: first immediately and optimistically on the client, then later authoritatively on the server. Replicache [automatically reconciles](/how-it-works#the-big-picture) differences between the optimistic and authoritative runs continuously on all clients.
- **[`src/app.tsx`](https://github.com/rocicorp/replicache-todo/blob/main/src/app.tsx)** subscribes to all todos in Replicache and renders them. It also wires up event handlers from the app to Replicache mutators.

# Next

Now [let's add a simple feature](/first-replicache-feature) to this app.
