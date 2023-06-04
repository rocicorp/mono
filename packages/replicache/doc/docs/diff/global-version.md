---
title: Global Version Strategy
slug: /concepts/diff/global-version
---

# 🌏 The Global Version Strategy

The Global Version Strategy is one of the easiest strategies to implement and the one we recomend most customers start with.

It does have concurrency limits because all pushes server-wide are serialized, and it doesn't support advanced features like incremental sync and read authorization as easily as [row versioning](/concepts/diff/row-version).

:::info

You may wonder why not use a timestamp for the version instead of a counter. While this would scale much better, it is not possible to implement correctly on most servers due to [unreliable clocks](https://www.ics.uci.edu/~cs230/lectures20/distrsyslectureset2-win20.pdf).

:::

## How it Works

### Setup

1. Add a `Version` field to each entity in your backend database.
2. Add a single `GlobalVersion` field to your database somewhere. This will be updated each time the database changes in some way.
3. Add storage for `ReplicacheClient`s in your backend database. Each client will have a `lastMutationID` field, the last mutation ID that the backend has processed.
4. Use _Soft Deletes_ for each entity in your backend database: add an `IsDeleted` field and set it to true when deleting an item rather than actually deleting it. Take this field into account when reading data.

### On Push

1. Open an exclusive (serializable) write transaction.
2. Read the global version and increment it to get the next version.
3. Read the client record for the requesting client from `ReplicacheClient`. If so such record exists, create one with `lastMutationID` defaulted to zero.
4. Process all mutations inside the lock and set the version field of all affected entities to the next version.
5. Update the `lastMutationID` for the requesting client to the last mutation processed.
6. Update the global version number.

:::caution

It is important that the push happen in a serialized transaction, and that both `lastMutationID` and `GlobalVersion` are updated atomically as part of this transaction. If this does not happen, clients can receive incorrect results. In particular, in Postgres, do not use a [sequence](https://www.postgresql.org/docs/current/sql-createsequence.html) for the global version, as sequences do not participate in transactions.

:::

:::info

It's a good idea to read the global version ["for update"](https://dev.mysql.com/doc/refman/8.0/en/innodb-locking-reads.html) if your database supports it. The reason is that this forces the database to wait for an exclusive lock, effectively queueing all pushes – sometimes referred to as the [semaphore](https://dev.mysql.com/doc/refman/5.7/en/innodb-deadlocks-handling.html) pattern.

If you don't do this, but still use serializable transactions, sync will be _correct_ (no data will be lost) but you will see deadlock errors in some databases when pushes and pulls overlap. This is because the database will first get a read lock on the version field, do some work, then later try to upgrade to a write lock, but discover some other transaction already has the write lock.

You can safely retry these deadlocked transactions (in fact Replicache will do this atuomatically), but it's more efficient and a better user experience to just avoid them. Eagerly getting a write lock in push avoid this problem.

:::

### On Pull

1. Open an exclusive (serializable) write transaction.
2. Read the global version.
3. Read the client record for the requesting client from `ReplicacheClient`, if any.
4. Calculate the `patch`:

- If the request cookie is `null`:
  - Read all entities where `IsDeleted=False`
  - Create a _reset patch_ - a patch with a `clear` op followed by `put` ops for each read entity
- Otherwise:
  - Read all entities whose `Version` is greater than the cookie value
  - Create a patch with `del` ops for each entity where `IsDeleted=True`, and `put` ops for other entities

5. Return the current global version, the requesting client's `lastMutationID` (or zero if no such client exists so far), and the patch.

## Challenges

### Performance

`GlobalVersion` functions as a global lock. This limits possible concurrency of your backend: if each push takes 20ms then the maximum number of pushes per second for your server is 50.

### Soft Deletes

Soft Deletes are annoying to maintain. All queries to the database need to be aware of the `IsDeleted` column and filter appropriately. There are other ways to track deletes however, see below.

### Read Authorization

In many applications, users only have access to a subset of the total data. If a user gains access to an entity they didn't previously have access to, pull should reflect that change. But that won't happen using just the logic described above, because the entity itself didn't change, and therefore its `Version` field won't change.

To correctly implement auth changes with this strategy, you also need to track those auth changes somehow — either by having those changes bump the `Version` fields of affected docs, or else by tracking changes to the auth rules themselves with their own `Version` fields.

## Variations

There are alternative mechanisms to implement Soft Deletes. For example, you can maintain a separate `Deleted` collection/table in the database. This removes the special case for writing queries at the cost of extra schema complexity in the database.

## Examples

The [Get Started Guide](/byob/remote-database) implements the Global Version strategy.
