---
title: Global Version Strategy
slug: /howto/diff/global-version
---

# 🌏 The Global Version Strategy

The Global Version Strategy is one of the easiest strategies to implement and the one we recomend most customer start with.

It does have concurrency limits because all pushes server-wide are serialized, and does not easily support more advanced features like incremental sync and read authorization.

## How it Works

### Setup

1. Add a `Version` field to each entity in your backend database.
2. Add a single `GlobalVersion` field to your database somewhere.
3. Use _Soft Deletes_ for each entity in your backend database: add an `IsDeleted` field and set it to true when deleting an item rather than actually deleting it. Take this field into account when reading data.

### On Push

1. Open an exclusive (serialized) write transaction.
2. Read the global version and increment it.
3. Process all mutations inside this lock and set the version field of all affected entities.
4. At the end of the push, update the global version number.

:::caution

It is important that the push happen in a serialized transaction, and that `GlobalVersion` is updated atomically as part of this transaction. If this does not happen, clients can receive incorrect results. In particular, in Postgres, do not use a [sequence](https://www.postgresql.org/docs/current/sql-createsequence.html) for the `Global Vesion`, as sequences do not participate in transactions.

:::

### On Pull

- If the request cookie is `null`:
  - Read all entities that where `IsDeleted=False`
  - Send a _reset patch_ - a patch with a `clear` op followed by `put` ops for each read entity
- Otherwise:
  - Read all entities whose `Version` is greater than the cookie value
  - Send `del` ops for each entity where `IsDeleted=True`, and `put` ops for other entities
  - Return as a cookie the current value of the `GlobalVersion` field in the requesting client's current client view

:::info

You may wonder why not use a timestamp for the version instead of a counter. While this would scale much better, it is not possible to implement correctly on most servers due to [unreliable clocks](https://www.ics.uci.edu/~cs230/lectures20/distrsyslectureset2-win20.pdf).

:::

## Challenges

### Performance

`GlobalVersion` functions as a global lock. This limits possible concurrency of your backend: if each push takes 20ms then the maximum number of pushes per second for your server is 50.

### Soft Deletes

Soft Deletes are annoying to maintain. All queries to the database need to be aware of the `IsDeleted` column and filter appropriately. There are other ways to track deletes however, see below.

### Read Authorization

In many applications, users only have access to a subset of the total data. If a user gains access to an entity they didn't previously have access to, pull should reflect that. But that change won't be captured in the entity's `Version` attribute as described above, because the entity itself didn't change — only whether the user had access to it did.

To correctly implement auth changes with this strategy, you also need to track those auth changes somehow — either by having auth changes bump the `Version` fields of affected docs, or by tracking changes to the auth rules themselves with their own `Version` fields.

## Variations

There are alternative mechanisms to implement Soft Deletes. For example, you can maintain a separate `Deleted` collection/table in the database. This removes the special case for writing queries at the cost of extra schema complexity in the database.
