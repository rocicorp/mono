---
title: Remote Schema
slug: /byob/remote-schema
---

There are a number of ways to implement Replicache backends.

The Replicache client doesn't actually care _how_ your backend works internally — it only cares that you provide correctly implemented [push](/reference/server-push.md) and [pull](/reference/server-pull.md) endpoints.

This walkthrough implements the [Global Version](/concepts/strategies/global-version) backend strategy, which is a simple strategy that we usually recommend users start with. See [Backend Strategies](/concepts/strategies/overview) for information on other commonly used strategies.

## Define the Schema

Let's define our Postgres schema. As suggested in the [Global Version Strategy](/concepts/strategies/global-version) doc, we'll track:

- **Global Version:** The version the backend database is currently at.
- **Clients:** Clients that have connected to the server, and the last mutationID processed from each. This is used during push to ensure mutations are processed only once, and in the order they happened on the client. We also store each client's `clientGroupID`, which is needed to correctly implement `pull`.
- **Domain Data:** The user data the application stores to do its job. Each stored item has a few extra Replicache-specific attributes:
  - `version`: The version of the containing space that this item was last updated at. Used to calculate a diff during pull.
  - `deleted`: A [soft delete](https://en.wiktionary.org/wiki/soft_deletion) used to communicate to clients during pull that a item was logically deleted.

Modify `db.ts` so that `initDb` looks like:

```ts
async function initDB() {
  console.log('initializing database...');
  const db = newDb().adapters.createPgPromise();
  await tx(async t => {
    // A single global version number for the entire database.
    await t.none(
      `create table replicache_server (id integer primary key not null, version integer)`,
    );
    await t.none(
      `insert into replicache_server (id, version) values ($1, 1)`,
      serverID,
    );

    // Stores chat messages.
    await t.none(`create table message (
      id text primary key not null,
      sender varchar(255) not null,
      content text not null,
      ord integer not null,
      deleted boolean not null,
      version integer not null)`);

    // Stores last mutationID processed for each Replicache client.
    await t.none(`create table replicache_client (
      id varchar(36) primary key not null,
      client_group_id varchar(36) not null,
      last_mutation_id integer not null,
      version integer not null)`);

    // TODO: indexes
  }, db);
  return db;
}
```

## Next

The [next section](./remote-mutations.md) implements remote mutations, so that our optimistic changes can become persitent.
