---
title: Share Mutators
slug: /howto/share-mutators
---

If your Replicache backend is written in JavaScript, it is possible to share mutator functions between client and server. This prevents you from having to write them twice.

This does require that your backend datamodel is key/value oriented like Replicache, so that the same code can run against both storage systems with minimal branching.

For example, our samples use PostgreSQL with a [single `entry` table](https://github.com/rocicorp/replicache-express/blob/main/src/backend/schema.ts#L31) having `text` `key` and `JSON` `value` columns. Another option would be to use a document database, like [Google Cloud Firestore](https://firebase.google.com/docs/firestore).

:::info

Although using a relational database as a document store is somewhat unconventional, Postgres has excellent JSON support and [does support this usage](https://www.postgresql.org/docs/current/datatype-json.html). This can be a very convenient way to get a Replicache project up and running quickly.

:::

# `replicache-transaction` Helper Package

We provide the [`replicache-transaction` package](https://www.npmjs.com/package/replicache-transaction) to make this usage easier. It adapts Replicache's `WriteTransaction` interface to some backend key/value storage that you provide. See [`PostgresTransaction`](https://github.com/rocicorp/replicache-express/blob/main/src/backend/postgres-storage.ts) in [`replicache-express`](https://github.com/rocicorp/replicache-express) for an example.

# Other Backend Datastores

If you want to use Replicache with some non-key/value backend datastore, such as a normalized SQL database, it typically makes more sense to implement the mutators twice.

See [Replicache on Rails](https://github.com/rocicorp/rails) for a JS helper library that can automate much of the client-side.
