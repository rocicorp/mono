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

A _Client View Record_ (CVR) captures minimal information about the state of the database at the time a Client View was generated.

In TypeScript, it might look like:

```ts
type CVR = {
  // Value of ReplicacheClientGroup.clientVersion at time of generation.
  clientVersion: number;

  // Map of key->version pairs, one for each entity in the client view.
  entities: Record<string, number>;
};
```

One CVR is generated for each pull response and stored in some ephemeral storage. The storage doesn’t need to be durable — if the CVR is lost, the server can just send a reset patch. And the storage doesn’t need to be transactional with the database. Redis is fine.

The CVRs are stored keyed under an incrementing ID which becomes the cookie sent to Replicache.

During pull, the server uses the cookie to lookup the CVR associated with the previous pull response. It then computes a new CVR for the latest server state and diffs the two CVRs to compute the delta to send to the client.

## Schema

```ts
type ReplicacheClientGroup = {
  // Same as the Reset Strategy.
  id: string;
  userID: any;

  // Incremented on each mutation from a client in the group.
  clientVersion: number;

  // Increments each time we generate a CVR.
  cvrVersion: number;
};

type ReplicacheClient = {
  // Same as the Reset Strategy.
  id: string;
  clientGroupID: string;
  lastMutationID: number;

  lastModifiedClientVersion: number;
};

// Each of your domain entities will have one extra field.
type Todo = {
  // ... fields needed for your application (id, title, complete, etc)

  // Incremented each time this row is updated.
  // Note this is not the same as the global or per-space versioning scheme.
  // Each entity has their *own* version which increments independently.
  version: number;
};
```

## Push

The push handler is similar to the Reset Strategy, except for with some modifications to track changes to clients and domain entities.

1. Create a new `ReplicacheClientGroup` if necessary.
1. Verify that the requesting user owns the specified `ReplicacheClientGroup`.

Then, for each mutation described in the [`PushRequest`](/reference/server-push#http-request-body):

<ol>
	<li value="3">Create the <code>ReplicacheClient</code> if necessary.</li>
	<li>Validate that the <code>ReplicacheClient</code> is part of the requested <code>ReplicacheClientGroup</code>.</li>
	<li>Increment the <code>clientVersion</code> field of the <code>ReplicacheClientGroup</code>.</li>
	<li>Validate that the received mutation ID is the next expected mutation ID from this client.</li>
	<li>Run the applicable business logic to apply the mutation.
		<ul>
			<li>Increment the <code>version</code> field of any affected domain entities.</li>
		</ul>
	</li>
	<li>Update the <code>lastMutationID</code> of the client to store that the mutation was processed.</li>
	<li>Update the <code>lastModifiedClientVersion</code> field of the client to the current <code>clientVersion</code> value.</li>
</ol>

## Pull

The pull logic is more involved than in other strategies because of the need to manage the CVRs:

<ol>
  <li>Verify that requesting user owns the requested <code>ReplicacheClientGroup</code>.</li>
	<li>Use the request cookie to fetch the corresponding CVR, or default to an empty CVR.</li>
	<li>Increment the <code>ReplicacheClientGroup</code> record's <code>cvrVersion</code> field.</li>
	<li>Fetch the ids and versions of the current Client View from the DB and use it to build the next CVR. This query can be any arbitrary function of the DB, including read authorization, paging, etc.</li>
	<li>Store the new CVR keyed by <code>clientGroupID</code> and the current <code>cvrVersion</code>.</li>
	<li>Fetch the values of all entities that are either new or have a greater version in the latest CVR.</li>
	<li>Fetch all <code>ReplicacheClient</code> records that have changed since the old CVR's <code>clientVersion</code>.</li>
  <li>Return a <code><a href="/reference/server-pull#http-response-body">PullResponse</a></code> with:
    <ul>
      <li><code>cvrVersion</code> as the cookie.</li>
      <li>The <code>lastMutatationID</code> for each changed client.</li>
      <li>A patch with:
        <ul>
          <li><code>put</code> ops for every created or changed entity.</li>
          <li><code>del</code> ops for every deleted entity.</li>
        </ul>
      </li>
    </ul>
  </li>
</ol>

## Example

See [todo-row-versioning](https://github.com/rocicorp/todo-row-versioning) for a complete example of this strategy, including sharing and dynamic authorization.

## Queries and Windowing

The query that builds the client view can change at any time, and can even be per-user. However, slight care must be taken because of the way that Replicache data is shared between tabs. Changing the pull query in one tab changes it for other tabs that are sharing the same Replicache. Without coordination, this could result in two tabs “fighting” over the current query.

The solution is to sync the current query with Replicache (🤯). That way it will be automatically synced to all tabs.

- Add a new entity to the backend database to store the current query for a profile. Like other entities it should have a `version` field. Let’s say: `/control/<profile-id>/query`.
- When computing the pull, first read this value. If not present, use the default query. Include this entity in the pull response as any other entity.
- In the UI can use the query data in the client view to check and uncheck filter boxes, etc., just like other Replicache data!
- Add mutations that modify this entity.

## Variations

- The CVR can be passed into the database as an argument enabling the pull to be computed in a single DB round-trip.
- The CVR can be **stored** in the primary database, allowing the pull to be computed with a single network round trip (no redis required). The downside is you must expire the CVR entries manually as you can’t rely on Redis caching.
- The per-row version number can also be a hash over the row serialization, or even a random GUID. These approaches might perform better in some datastores since it eliminates a read of the existing row during write.
- The changed clients can also be computed using the CVR approach rather than the <code>clientVersion</code>. The downside to doing that is that the set of Clients per ClientGroup only grows, and so the size of the diff in memory in the app server grows forever too.
- It is still totally fine to partition data into spaces and have a per-space `version` column ([aka a “semaphore”](https://dev.mysql.com/doc/refman/5.7/en/innodb-deadlocks-handling.html)) to enforce serialization and avoid deadlocks. This just becomes orthogonal to computing pulls.
