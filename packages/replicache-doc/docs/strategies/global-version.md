---
title: Global Version Strategy
slug: /strategies/global-version
---

# 🌏 The Global Version Strategy

A single global `version` is stored in the database and incremented on each push. Entities have a `lastModifiedVersion` field which is the global version the entity was last modified at.

The global version is returned as the cookie to Replicache in each pull, and sent in the request of the next pull. Using this we can find all entities that have changed since the last pull and calculate the correct patch.

While simple, the Global Version Strategy does have concurrency limits because all pushes server-wide are serialized, and it doesn't support advanced features like incremental sync and read authorization as easily as [row versioning](/strategies/row-version).

## Schema

The schema builds on the schema for the [Reset Strategy](./reset.md), and adds a few things to support the global version concept.

```ts
// Tracks the current global version of the database. There is only one of
// these system-wide.
type ReplicacheSpace = {
  version: number;
};

type ReplicacheClientGroup = {
  // Same as Reset Strategy.
  id: string;
  userID: any;
};

type ReplicacheClient = {
  // Same as Reset Strategy.
  id: string;
  clientGroupID: string;
  lastMutationID: number;

  // The global version this client was last modified at.
  lastModifiedVersion: number;
};

// Each of your domain entities will have two extra fields.
type Todo = {
  // ... fields needed for your application (id, title, complete, etc)

  // The global version this entity was last modified at.
  lastModifiedVersion: number;

  // "Soft delete" for marking whether this entity has been deleted.
  deleted: boolean;
};
```

## Push

The push handler is the same as in the Reset Strategy, but with changes to mark domain entities with the version they were changed at. The changes from the Reset Strategy are marked below **in bold**.

Replicache sends a [`PushRequest`](/reference/server-push#http-request-body) to the push endpoint. For each mutation described in the request body, the push endpoint should:

1. `let errorMode = false`
1. Begin transaction
1. Read the `ReplicacheClientGroup` for `body.clientGroupID` from the database, or default to:

```json
{
  id: body.clientGroupID,
  userID
}
```

4. Verify the requesting user owns the specified client group.
1. **Read the `ReplicacheClient` for `mutation.clientID` or default to:**

```json
{
  id: mutation.clientID,
  clientGroupID: body.clientGroupID,
  lastMutationID: 0,
  lastModifiedVersion
}
```

6. Verify the requesting client group owns the requested client.
1. `let nextMutationID = client.lastMutationID + 1`
1. **Read the global `ReplicacheSpace`.**
1. **`let nextVersion = replicacheSpace.version + 1`**
1. Rollback transaction and skip this mutation if already processed (`mutation.id < nextMutationID`)
1. Rollback transaction and error if mutation from the future (`mutation.id > nextMutationID`)
1. If `errorMode != true` then:
   1. Try to run business logic for mutation
      1. **Set `lastModifiedVersion` for any modified rows to `nextVersion`.**
      1. **Set `deleted = true` for any deleted entities.**
   1. If error:
      1. Log error
      1. `set errorMode = true`
      1. Abort transaction
      1. Repeat these steps at the beginning
1. **Write `ReplicacheSpace`:**

```json
{
  version: nextVersion,
}
```

14. Write `ReplicacheClientGroup`:

```json
{
  id: body.clientGroupID,
  userID,
}
```

15. **Write `ReplicacheClient`:**

```json
{
  id: mutation.clientID,
  clientGroupID: body.clientGroupID,
  lastMutationID: nextMutationID,
  lastModifiedVersion: nextVersion,
}
```

16. Commit transaction

After the loop is complete, poke clients to cause them to pull.

## Pull

The pull handler is the same as in the Reset Strategy, but with changes to read only entities that are newer than the last pull. The changes from the Reset Strategy are marked below **in bold**.

Replicache sends a [`PullRequest`](/reference/server-pull#http-request-body) to the pull endpoint. The endpoint should:

1. Begin transaction
1. **`let prevVersion = body.cookie ?? 0`**
1. Read the `ReplicacheClientGroup` for `body.clientGroupID` from the database, or default to:

```json
{
  id: body.clientGroupID,
  userID
}
```

4. Verify the requesting client group owns the requested client.
1. **Read the global `ReplicacheSpace` entity**
1. **Read all domain entities from the database that have `lastModifiedVersion > prevVersion`**
1. **Read all `ReplicacheClient` records for the requested client group that have `lastModifiedVersion > prevVersion`.**
1. Create a `PullResponse` with:
   1. **`cookie` set to `space.version`**
   1. **`lastMutationIDChanges` set to the `lastMutationID` for every client that has changed.**
   1. `patch` set to:
      1. **`op:del` for all domain entities that have changed and are deleted**
      1. **`op:put` for all domain entities that have changed and aren't deleted**

## Example

See [todo-nextjs](https://github.com/rocicorp/todo-nextjs) for an example of this strategy. Note that this sample also uses [Shared Mutators](../howto/share-mutators) and batches the mutations into a single transaction. So the logic is a little different than above, but equivalent.

## Why Not Use Last-Modified?

When presented with the pull endpoint, most developers' first instinct will be to implement it using last-modified timestamps. This can't be done correctly, and we strongly advise against trying. Here's why:

<p align="center">
  <img src="/img/please-dont-use-last-modified.png" width="80%"/>
</p>

Imagine that a Replicache client `c1` sends a push `p1`. The server receives `p1` at time `t1` and begins processing the push, updating all changed records with `lastModified = t1`.

While the push is being processed, some other client `c2` sends a pull `p2`. The server receives the pull at time `t2` and processes it, returning all changes necessary to bring `c2` up to `t2`.

Finally, `p1` completes and commits, writing new records with timestamp `t1`.

Now `c2` thinks it has changes up to `t2`, but is actually missing the ones from `p1`. This problem will never resolve. On the next pull, `c2` will send timestamp `t2`. The server won't send the missing changes since they have an earlier timestamp. Unlike in a traditional web app, a refresh won't solve this problem. On refresh, Replicache will just read the incorrectly cached data from the browser.

In local-first systems it's important to ensure correct synchronization, since cached data is permanent. The problem with using last-modified timestamps is that the linear nature of timestamps assumes a linear series of modifications to the database. But databases don't work that way – they can (and often do) do things in parallel.

The Global Version strategy resolves this problem by forcing the database to process pushes serially, making a single monotonic integer cookie sufficient to represent the state of the DB. The [Row Version](./row-version) strategy resolves it by using a cookie that can correctly represent DB state, even with parallel execution.

## Challenges

### Performance

`GlobalVersion` functions as a global lock. This limits possible concurrency of your backend: if each push takes 20ms then the maximum number of pushes per second for your server is 50.

### Soft Deletes

Soft Deletes are annoying to maintain. All queries to the database need to be aware of the `deleted` column and filter appropriately. There are other ways to implement soft deletes (see below), but they are all at least a little annoying.

### Read Authorization

In many applications, users only have access to a subset of the total data. If a user gains access to an entity they didn't previously have access to, pull should reflect that change. But that won't happen using just the logic described above, because the entity itself didn't change, and therefore its `lastModifiedVersion` field won't change.

To correctly implement auth changes with this strategy, you also need to track those auth changes somehow — either by having those changes bump the `lastModifiedVersion` fields of affected docs, or else by tracking changes to the auth rules themselves with their own `lastModifiedVersion` fields.

## Variations

### Early Exit, Batch Size

Just as in the Reset strategy, you can [early exit](./reset#early-exit) the push handler or process mutations in [batches](./reset#batching).

### Alternative Soft Delete

There are other ways to implement soft deletes. For example for each entity in your system you can have a separate collection of just deleted entities:

```ts
type Monster = {
  // other fields ...

  // note: no `deleted` here

  // The version of the database this entity was last changed during.
  replicacheVersion: number;
};

type MonsterDeleted = {
  // The version of the db the monster was deleted at
  replicacheVersion: number;
};
```

This makes read queries more natural (can just query Monsters collection as normal). But deletes are still weird (must upsert into the `MonstersDeleted` collection).
