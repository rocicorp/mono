---
title: Local Postgres
slug: /local-postgres
---

By default the starter app backend stores data in memory using [`pg-mem`](https://github.com/oguimbal/pg-mem). This is convenient during development, but you may also want to test against a real Postgres sometimes.

To do so:

1. [Install Postgres](https://www.postgresql.org/).
2. Create a database for replicache-todo to use, i.e., `psql -d postgres -c 'create database todo'`
3. Launch replicache-todo like `DATABASE_URL=<database-connection-string> npm run dev`
