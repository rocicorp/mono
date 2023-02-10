---
title: Quickstarts
slug: /quickstarts
---

Templates to get you up and running quickly with a variety of frameworks.

## React/TypeScript/Express

```bash
npx degit rocicorp/todo-react my-app
cd my-app
npm install

# Get a license if you don't have one already:
# https://doc.replicache.dev/howto/licensing

VITE_REPLICACHE_LICENSE_KEY=<your-license> npm run watch --ws
```

## Web Components/TypeScript/Express

```bash
npx degit rocicorp/todo-wc my-app
cd my-app
npm install

# Get a license if you don't have one already:
# https://doc.replicache.dev/howto/licensing

VITE_REPLICACHE_LICENSE_KEY=<your-license> npm run watch --ws
```

## React/TypeScript/NextJS

```bash
npx degit rocicorp/replicache-todo my-app
cd my-app
npm install

# Get a license if you don't have one already:
# https://doc.replicache.dev/howto/licensing

NEXT_PUBLIC_REPLICACHE_LICENSE_KEY=<your-license> npm run dev

# If you want to run against Supabase, you need to set up a [Supabase project](https://supabase.com/) and
# set the following environment variables:

NEXT_PUBLIC_SUPABASE_ANON_KEY
SUPABASE_DATABASE_PASSWORD
NEXT_PUBLIC_SUPABASE_URL
```
