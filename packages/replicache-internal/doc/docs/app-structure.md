---
title: App Structure
slug: /app-structure
---

# A Quick Tour of the Starter App

The starter app utilizes [npm workspaces](https://docs.npmjs.com/cli/v8/using-npm/workspaces) to help organize the project into a few different packages:

- **`client`** (frontend)
- **`shared`** (common code between client and server: mutators, and todo model)
- **`server`** (backend)

Here are a few key bits of code to look at to get a feel for how Replicache works:

- **[`shared/src/todo.ts`](https://github.com/rocicorp/replicache-quickstarts/tree/main/shared/src/todo.ts)** defines the todo entity. You will typically have one such file for each entity in your application.
- **[`shared/src/mutators.ts`](https://github.com/rocicorp/replicache-quickstarts/tree/main/shared/src/mutators.ts)** defines the [Replicache mutators](/how-it-works#mutations) for the todo app. Replicache mutators run twice: first immediately and optimistically on the client, then later authoritatively on the server. Replicache [automatically reconciles](/how-it-works#the-big-picture) differences between the optimistic and authoritative runs continuously on all clients.
- **[`client/react/src/index.tsx`](https://github.com/rocicorp/replicache-quickstarts/tree/main/client/react/src/index.tsx)** is the app's entrypoint. It initializes Replicache, and generates a new random todo list ID.
- **[`client/react/src/app.tsx`](https://github.com/rocicorp/replicache-quickstarts/tree/main/client/react/src/app.tsx)** is the top-level component. It subscribes to all todos in Replicache and renders them. It also wires up event handlers from the app to Replicache mutators.
- **[`server/src/server.ts`](https://github.com/rocicorp/replicache-quickstarts/tree/main/server/src/server.ts)** is a simple Replicache server built with [replicache-express](https://www.npmjs.com/package/replicache-express). It handles all of the backend requests required by Replicache. (poke, pull, push, createSpace, spaceExists)

# Next

Now [let's add a simple feature](/first-replicache-feature) to this app.
