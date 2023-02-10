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

This is a really simple Replicache app using React, Next.js, and Supabase.

<p class="text--center">
  <img src="/img/setup/todo.webp" width="650"/>
</p>

For detailed instructions on building a Replicache app, see the [integration guide](/guide/intro).
