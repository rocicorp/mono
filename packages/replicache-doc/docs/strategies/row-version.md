---
title: Row Version Strategy
slug: /strategies/row-version
---

# 🚣 The Row Version Strategy

This strategy has a few big advantages over the other strategies:

- The Client View can be **computed** dynamically — it can be any arbitrary query over the database, including filters, joins, windows, auth, etc. This _pull query_ can even change per-user. If the user checks a box in the UI, the query might change from _“all active threads"_ to _"all active threads, or first 20 inactive threads ordered by modified-date”_.
- It does not require global locks or the concept of spaces.
- It does not require a soft deletes. Entities can be fully deleted.

The disadvantage is that it pays for this flexibility in increased implementation complexity and read cost. Pulls become more expensive because they require a few queries, and they aren’t a simple index scan. However because there are no global locks, the database should be easier to scale.

## Client View Records

A _Client View Record_ (CVR) is a minimal representation of a Client View snapshot. In other words, it captures what data a Client Group had at a particular moment in time.

In TypeScript, it might look like:

```ts
type CVR = {
  id: string;
  // Map of clientID->lastMutationID pairs, one for each client in the
  // client group.
  lastMutationIDs: Record<string, number>;
  // Map of key->version pairs, one for each entity in the client view.
  entities: Record<string, number>;
};
```

One CVR is generated for each pull response and stored in some ephemeral storage. The storage doesn’t need to be durable — if the CVR is lost, the server can just send a reset patch. And the storage doesn’t need to be transactional with the database. Redis is fine.

The CVRs are stored keyed under a random unique ID which becomes the cookie sent to Replicache.

During pull, the server uses the cookie to lookup the CVR associated with the previous pull response. It then computes a new CVR for the latest server state and diffs the two CVRs to compute the delta to send to the client.

## Schema

```ts
type ReplicacheClientGroup = {
  // Same as the Reset Strategy.
  id: string;
  userID: any;

  // Replicache requires that cookies are ordered within a client group.
  // To establish this order we simply keep a counter.
  cvrVersion: number;
};

type ReplicacheClient = {
  // Same as the Reset Strategy.
  id: string;
  clientGroupID: string;
  lastMutationID: number;
};

// Each of your domain entities will have one extra field.
type Todo = {
  // ... fields needed for your application (id, title, complete, etc)

  // Incremented each time this row is updated.
  // In Postgres, there is no need to declare this as Postgres tracks its
  // own per-row version 'xmin' which we can use for this purpose:
  // https://www.postgresql.org/docs/current/ddl-system-columns.html
  version: number;
};
```

## Push

The push handler is similar to the Reset Strategy, except for with some modifications to track changes to clients and domain entities. The changes from the Reset Strategy are marked **in bold**.

Replicache sends a [`PushRequest`](/reference/server-push#http-request-body) to the push endpoint. For each mutation described in the request body, the push endpoint should:

1. `let errorMode = false`
1. Begin transaction
1. **`getClientGroup(body.clientGroupID)`, or default to:**

```json
{
  id: body.clientGroupID,
  userID
  cvrVersion: 0,
}
```

4. Verify requesting user owns specified client group.
1. `getClient(mutation.clientID)` or default to:

```json
{
  id: mutation.clientID,
  clientGroupID: body.clientGroupID,
  lastMutationID: 0,
}
```

6. Verify requesting client group owns requested client
1. `let nextMutationID = client.lastMutationID + 1`
1. Rollback transaction and skip mutation if already processed (`mutation.id < nextMutationID`)
1. Rollback transaction and error if mutation from future (`mutation.id > nextMutationID`)
1. If `errorMode != true` then:
   1. Try business logic for mutation
      1. **Increment `version` for modified rows**
      1. Note: Soft-deletes _not_ required – you can delete rows normally as part of mutations
   1. If error:
      1. Log error
      1. Abort transaction
      1. Retry this transaction with `errorMode = true`
1. **`putClientGroup()`**:

```json
{
  id: body.clientGroupID,
  userID,
  cvrVersion: clientGroup.cvrVersion,
}
```

12. `putClient()`:

```json
{
  id: mutation.clientID,
  clientGroupID: body.clientGroupID,
  lastMutationID: nextMutationID,
}
```

13. Commit transaction

After the loop is complete, poke clients to cause them to pull.

## Pull

The pull logic is more involved than other strategies because of the need to manage the CVRs.

Replicache sends a [`PullRequest`](/reference/server-pull#http-request-body) to the pull endpoint. The endpoint should:

1. `let prevCVR = getCVR(body.cookie.cvrID)`
1. `let baseCVR = prevCVR` or default to:

```json
{
  "id": "",
  "entries": {}
}
```

3. Begin transaction
1. `getClientGroup(body.clientGroupID)`, or default to:

```json
{
  id: body.clientGroupID,
  userID,
  cvrVersion: 0,
}
```

5. Verify requesting client group owns requested client.
1. Read all id/version pairs from the database that should be in the client view. This query can be any arbitrary function of the DB, including read authorization, paging, etc.
1. Read all clients in the client group.
1. Build `nextCVR` from entities and clients.
1. Calculate the difference between `baseCVR` and `nextCVR`
1. If prevCVR was found and two CVRs are identical then exit this transaction and return a no-op PullResopnse to client:

```json
{
  cookie: prevCookie,
  lastMutationIDChanges: {},
  patch: [],
}
```

11. Fetch all entities from database that are new or changed between `baseCVR` and `nextCVR`
1. `let clientChanges = clients that are new or changed since baseCVR`
1. `let nextCVRVersion = Math.max(pull.cookie?.order ?? 0, clientGroup.cvrVersion) + 1`

:::caution

It's important to default to the incoming cookie's order because when
Replicache creates a new ClientGroup, it can fork from an existing one,
and we need the order to not go backward.

:::

14. `putClientGroup()`:

```json
{
  id: clientGroup.id,
  userID: clientGroup.userID,
  cvrVersion: nextCVRVersion,
}
```

15. Commit
1. `let nextCVRID = randomID()`
1. `putCVR(nextCVR)`
1. Create a `PullResponse` with:
   1. A patch with:
      1. `op:clear` if `prevCVR === undefined`
      1. `op:put` for every created or changed entity
      1. `op:del` for every deleted entity
   1. `{order: nextCVRVersion, cvrID}` as the cookie.
   1. `lastMutationIDChanges` with entries for every client that has changed.

## Example

See [todo-row-versioning](https://github.com/rocicorp/todo-row-versioning) for a complete example of this strategy, including sharing and dynamic authorization.

## Queries and Windowing

The query that builds the client view can change at any time, and can even be per-user. However, slight care must be taken because of the way that Replicache data is shared between tabs. Changing the pull query in one tab changes it for other tabs that are sharing the same Replicache. Without coordination, this could result in two tabs “fighting” over the current query.

The solution is to sync the current query with Replicache (🤯). That way it will be automatically synced to all tabs.

- Add a new entity to the backend database to store the current query for a profile. Like other entities it should have a `version` field. Let’s say: `/control/<userid>/query`.
- When computing the pull, first read this value. If not present, use the default query. Include this entity in the pull response as any other entity.
- In the UI can use the query data in the client view to check and uncheck filter boxes, etc., just like other Replicache data!
- Add mutations that modify this entity.

## Variations

- The CVR can be passed into the database as an argument enabling the pull to be computed in a single DB round-trip.
- The CVR can be **stored** in the primary database, allowing the patch to be computed with database joins and dramatically reducing amount of data read from DB.
- The per-row version number can also be a hash over the row serialization, or even a random GUID. These approaches might perform better in some datastores since it eliminates a read of the existing row during write.
