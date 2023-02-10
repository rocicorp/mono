---
title: Deploy Elsewhere
slug: /deploy-elsewhere
---

The quickstart app is a standard React app with a Postgres database backend, and can therefore be deployed just about anywhere a Node app can be deployed.

The one slightly tricky bit is that Replicache needs the server to send a server-to-client [poke message](https://doc.replicache.dev/how-it-works) when something has changed, telling Replicache to pull again.

The [Deploy on Render](/deploy-render) and [Deploy on Vercel and Supabase](/deploy-vercel-supabase) demonstrate two possible configurations that should be easy to adapt to many providers and situations.

If you need help deploying Replicache, please [Contact Us](https://replicache.dev/contact).
