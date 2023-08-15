---
title: Global Version Strategy
slug: /strategies/global-version
---

# 🌏 The Global Version Strategy

A single global `version` is stored in the database and incremented on each push. Entities have a `lastModifiedVersion` field which is the global version the entity was last modified at.

The global version is returned as the cookie to Replicache in each pull, and sent in the request of the next pull. Using this we can find all entities that have changed since the last pull and calculate the correct patch.

While simple, the Global Version Strategy does have concurrency limits because all pushes server-wide are serialized, and it doesn't support advanced features like incremental sync and read authorization as easily as [row versioning](/strategies/row-version).

:::info

You may wonder why not use a timestamp for the version instead of a counter. While this would scale much better, it is not possible to implement correctly on most servers due to [unreliable clocks](https://www.ics.uci.edu/~cs230/lectures20/distrsyslectureset2-win20.pdf).

:::

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

The push handler is the same as the Reset Strategy, but with changes to annotate entities with the version they were changed at.

1. Create a new `ReplicacheClientGroup` if necessary.
1. Verify that the requesting user owns the specified `ReplicacheClientGroup`.

Then, for each mutation described in the [`PushRequest`](/reference/server-push#http-request-body):

<ol>
  <li value="3">Create the <code>ReplicacheClient</code> if necessary.</li>
  <li>Validate that the <code>ReplicacheClient</code> is part of the requested <code>ReplicacheClientGroup</code>.</li>
  <li>Validate that the received mutation ID is the next expected mutation ID from this client.</li>
  <li>Increment the global version.</li>
  <li>Run the applicable business logic to apply the mutation.
    <ul>
      <li>For each domain entity that is changed or deleted, update its <code>lastModifiedVersion</code> to the current global version.</li>
      <li>For each domain entity that is deleted, set its <code>deleted</code> field to true.</li>
    </ul>
  </li>
  <li>Update the <code>lastMutationID</code> of the client to store that the mutation was processed.</li>
  <li>Update the <code>lastModifiedVersion</code> of the client to the current global version.</li>
</ol>

As with the Reset Strategy, it's important that each mutation is processed within a serializable transaction.

### Pull

<ol>
  <li>Verify that requesting user owns the requested <code>ReplicacheClientGroup</code>.</li>
  <li>Return a <code><a href="/reference/server-pull#http-response-body">PullResponse</a></code> with:
    <ul>
      <li>The current global version as the cookie.</li>
      <li>The <code>lastMutatationID</code> for each client that has changed since the requesting cookie.</li>
      <li>A patch with:
        <ul>
          <li><code>put</code> ops for every entity created or changed since the request cookie.</li>
          <li><code>del</code> ops for every entity deleted since the request cookie.</li>
        </ul>
      </li>
    </ul>
  </li>
</ol>

## Example

See [todo-nextjs](https://github.com/rocicorp/todo-nextjs) for an example of this strategy.

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

Just as in the Reset strategy, you can [early exit](./reset#early-exit) the push handler or process mutations in [smaller batches](./reset#batch-size).

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
