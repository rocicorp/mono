---
title: Per-Space Version Strategy
slug: /strategies/per-space-version
---

# 🛸 Per-Space Version Strategy

The Per-Space Version Strategy is the same as the [The Global Version Strategy](/strategies/global-version) except it has more than one space.

This increases throughput of the server. Instead of approximately 50 pushes per second across your entire server, you can get 50 pushes per space.

A common example of how people partition by space is along organizational boundaries in a SaaS application. Each customer org would be its own space and you'd thereby get 50 pushes per second per organization.

The tradeoffs to keep in mind is that you lose consistency guarantees across spaces. Replicache mutations are atomic: you can move data within a space, rename, copy, etc., and you have a guarantee that the entire change happens or none of it does. But this guarantee does not apply across spaces.

:::tip Example

Imagine moving data from one space to another. Because there is no transactional guarantees across spaces, during the move, the user might see the data exist in both spaces, or neither.

While this might just seem like a minor UI annoyance, keep in mind that it means that if you have IDs that refer to data across spaces, there is no guarantee that the data actually exists at the moment you render. You'll have to defensively guard against invalid pointers into other spaces.

:::

This is why partitioning makes most sense at very high-level boundaries, like organizations, so that it will be uncommon in your application to want to have data from two spaces interact.

## Schema

The schema generalizes the schema from the [Global Version Strategy](./reset.md):

```ts
type ReplicacheSpace = {
  id: string;

  // Same as Global Version Strategy.
  version: number;
};

type ReplicacheClientGroup = {
  // Same as Global Version Strategy.
  id: string;
  userID: any;

  spaceID: string;
};

type ReplicacheClient = {
  // Same as Global Version Strategy.
  id: string;
  clientGroupID: string;
  lastMutationID: number;
  lastModifiedVersion: number;
};

// Each of your domain entities will have three additional fields.
type Todo = {
  // ... fields needed for your application (id, title, complete, etc)

  // Same as Global Version Strategy.
  lastModifiedVersion: number;
  deleted: boolean;

  spaceID: string;
};
```

## Push

The push handler should receive the `spaceID` being operated on as an HTTP parameter. The logic is otherwise almost identical to the Global Version Strategy, with minor changes to deal with spaces. The changes from the Global Version Strategy are marked below **in bold**.

Replicache sends a [`PushRequest`](/reference/server-push#http-request-body) to the push endpoint. For each mutation described in the request body, the push endpoint should:

1. `let errorMode = false`
1. Begin transaction
1. **Read the `ReplicacheClientGroup` for `body.clientGroupID` from the database, or default to:**

```json
{
  id: body.clientGroupID,
  spaceID,
  userID
}
```

4. Verify the requesting user owns the specified client group.
1. **Verify the specified client group is part of the requesting space.**
1. Read the `ReplicacheClient` for `mutation.clientID` or default to:

```json
{
  id: mutation.clientID,
  clientGroupID: body.clientGroupID,
  lastMutationID: 0,
  lastModifiedVersion
}
```

7. Verify the requesting client group owns the requested client.
1. `let nextMutationID = client.lastMutationID + 1`
1. **Read the `ReplicacheSpace` for `request.params.spaceID`**
1. `let nextVersion = replicacheSpace.version + 1`
1. Rollback transaction and skip this mutation if already processed (`mutation.id < nextMutationID`)
1. Rollback transaction and error if mutation from the future (`mutation.id > nextMutationID`)
1. If `errorMode != true` then:
   1. Try to run business logic for mutation
      1. Set `lastModifiedVersion` for any modified rows to `nextVersion`.
      1. Set `deleted = true` for any deleted entities.
   1. If error:
      1. Log error
      1. `set errorMode = true`
      1. Abort transaction
      1. Repeat these steps at the beginning
1. **Write `ReplicacheSpace`:**

```json
{
  id: body.clientGroupID,
  spaceID: request.params.spaceID,
  version: nextVersion,
}
```

15. **Write `ReplicacheClientGroup`:**

```json
{
  id: body.clientGroupID,
  userID,
  spaceId,
}
```

16. Write `ReplicacheClient`:

```json
{
  id: mutation.clientID,
  clientGroupID: body.clientGroupID,
  lastMutationID: nextMutationID,
  lastModifiedVersion: nextVersion,
}
```

17. Commit transaction

After the loop is complete, poke clients to cause them to pull.

## Pull

The pull handler is the same as in the Global Version Strategy, but with mionr changes to support multiple spaces. Changes from the Global Version Strategy are **marked in bold**.

Replicache sends a [`PullRequest`](/reference/server-pull#http-request-body) to the pull endpoint. The pull handler should also receive the `spaceID` being operated on as an HTTP parameter. The endpoint should:

1. Begin transaction
1. `let prevVersion = body.cookie ?? 0`
1. Read the `ReplicacheClientGroup` for `body.clientGroupID` from the database, or default to:

```json
{
  id: body.clientGroupID,
  userID
}
```

4. Verify the requesting client group owns the requested client.
1. Verify the client group is part of the requesed space.
1. **Read the `ReplicacheSpace` entity for `request.params.spaceID`**
1. **Read all domain entities from the database that have `spaceID == request.params.spaceID AND lastModifiedVersion > prevVersion`**
1. Read all `ReplicacheClient` records for the requested client group that have `lastModifiedVersion > prevVersion`.
1. Create a `PullResponse` with:
   1. `cookie` set to `space.version`
   1. `lastMutationIDChanges` set to the `lastMutationID` for every client that has changed.
   1. `patch` set to:
      1. `op:del` for all domain entities that have changed and are deleted
      1. `op:put` for all domain entities that have changed and aren't deleted

## Example

[Todo-WC](https://github.com/rocicorp/todo-wc) is a simple example of per-space versioning. [Repliear](/examples/repliear) is a more involved example. Note that both examples also uses [Shared Mutators](../howto/share-mutators) and batch the mutations into a single transaction. So the logic is a little different than described above, but equivalent.

## Challenges

- Like the Global Version strategy, soft deletes can be annoying.
- Also like the Global Version strategy, it is difficult to implement features like read authentication and partial sync.
- It can be hard in some applications to find a way to partition spaces naturally.
- 50 pushes per second per space can still be insufficient for some applications.

## Variations

The same variations available to [The Global Version Strategy](/strategies/global-version#variations) apply here.
