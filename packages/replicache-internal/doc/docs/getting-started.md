---
title: Getting Started
slug: /
---

The easiest way to get started is with our Todo starter app. For information about
the license key step, see [Licensing](/licensing).

```bash
# Get a Replicache license key
npx replicache get-license

# Clone the repo and start supabase
git clone https://github.com/rocicorp/replicache-todo
cd replicache-todo
npm install
supabase init
supabase start

# Use license key printed out by `npx replicache get-license`
REPLICACHE_LICENSE_KEY="<license key>" \
# Use URLs and keys printed out by `supabase start`
DATABASE_URL="<DB URL>" \
NEXT_PUBLIC_SUPABASE_URL="<API URL>" \
NEXT_PUBLIC_SUPABASE_KEY="<anon key>" \
npm run dev
```

Tada! You now have a simple todo app powered by Replicache, <a href="https://nextjs.org/">Next.js</a>, and <a href="https://supabase.com/">Supabase</a>.

<p class="text--center">
  <img src="/img/setup/todo.webp" width="650"/>
</p>

You can start modifying this app to build something new with Replicache.

## A Quick Tour of the Starter App

- **[`frontend/`](https://github.com/rocicorp/replicache-todo/blob/main/frontend)** contains the UI. This is mostly a standard React/Next.js application.
- **[`frontend/mutators.ts`](https://github.com/rocicorp/replicache-todo/blob/main/frontend/mutators.ts)** defines the _mutators_ for this application. This is how you write data using Replicache. Call these functions from the UI to add or modify data. The mutations will be pushed to the server in the background automatically.
- **[`frontend/app.tsx`](https://github.com/rocicorp/replicache-todo/blob/main/frontend/app.tsx)** subscribes to all the todos in Replicache using `useSubscribe()`. This is how you typically build UI using Replicache: the hook will re-fire when the result of the subscription changes, either due to local (optimistic) changes, or changes that were synced from the server.
- **[`backend/`](https://github.com/rocicorp/replicache-todo/blob/main/backend)** contains a simple, generic Replicache server that stores data in Supabase. You don't need to worry about or touch this directory initially because the server is implemented automatically by reusing the mutators defined in `frontend/mutators.ts`. When you want to do more advanced things (e.g., mutators that work differently on the server) you'll modify this directory.

## Next

To understand the big picture of how to use Replicache, see [How Replicache Works](./how-it-works.md).
