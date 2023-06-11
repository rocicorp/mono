---
title: Remote Schema
slug: /byob/remote-schema
---

There are a number of ways to implement Replicache backends.

The Replicache client doesn't actually care _how_ your backend works internally — it only cares that you provide correctly implemented [push](/reference/server-push.md) and [pull](/reference/server-pull.md) endpoints.

This walkthrough implements the [Global Version](/concepts/diff/global-version) backend strategy, which is a simple strategy that we usually recommend users start with. See [Backend Strategies](/concepts/diff/overview) for information on other commonly used strategies.

## Define the Schema

Let's define our Postgres schema. As suggested in the [Global Version Strategy](/concepts/diff/global-version) doc, we'll track:

- **Global Version:** The version the backend database is currently at.
- **Clients:** Clients that have connected to the server, and the last mutationID processed from each. This is used during push to ensure mutations are processed only once, and in the order they happened on the client.
- **Domain Data:** The user data the application stores to do its job. Each stored item has a few extra Replicache-specific attributes:
  - `version`: The version of the containing space that this item was last updated at. Used to calculate a diff during pull.
  - `deleted`: A [soft delete](https://en.wiktionary.org/wiki/soft_deletion) used to communicate to clients during pull that a item was logically deleted.

Modify `db.js` so that `initDb` looks like:

```js
async function initDb() {
  const db = newDb().adapters.createPgPromise();
  await tx(async t => {
    // A single global version number for the entire database.
    await t.none(`create table replicache_version (version integer)`);
    await t.none(`insert into replicache_version (version) values (0)`);

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
            last_mutation_id integer not null)`);
  }, db);
  return db;
}
```

## Next

The [next section](./remote-mutations.md) implements remote mutations, so that our optimistic changes can become persitent.
