---
title: Quickstarts
slug: /quickstarts
---

We offer quickstarts to get you up and running quick in a variety of frameworks.

## React/TypeScript/Express

```bash
npx create-replicache-app my-app react
cd my-app
npm install

# Get a license if you don't have one already:
# https://doc.replicache.dev/licensing

VITE_REPLICACHE_LICENSE_KEY=<your-license> npm run watch --ws
```

## Web Components/TypeScript/Express

```bash
npx create-replicache-app my-app ts-web-component
cd my-app
npm install

# Get a license if you don't have one already:
# https://doc.replicache.dev/licensing

VITE_REPLICACHE_LICENSE_KEY=<your-license> npm run watch --ws
```

## React/TypeScript/NextJS

```bash
npx create-replicache-app my-app nextjs
cd my-app
npm install

# Get a license if you don't have one already:
# https://doc.replicache.dev/licensing

NEXT_PUBLIC_REPLICACHE_LICENSE_KEY=<your-license> npm run dev --ws

# If you want to run against Supabase, you need to set up a [Supabase project](https://supabase.com/) and
# set the following environment variables:

NEXT_PUBLIC_SUPABASE_ANON_KEY
SUPABASE_DATABASE_PASSWORD
NEXT_PUBLIC_SUPABASE_URL
```
