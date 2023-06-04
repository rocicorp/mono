---
title: Row Version Diff Strategy
slug: /howto/diff/row-version
---

# 🚣‍♀️ The Row Version Strategy

This strategy has a few big advantages over most other strategies:

- The client view can be **computed** dynamically — it can be any arbitrary query over the database, including filters, joins, windows, auth, etc. This **pull query** can even change per-user. If the user checks something in the UI, it can change to “all active threads, or first 20 inactive threads ordered by modified-date”.
- It does not require a global lock or the concept of spaces.
- It does not require a soft deletes (the `deleted` column). Entities can be fully deleted.

The disadvantage is that it pays for this flexibility in increased implementation complexity and read cost. Pulls become more expensive because they require a few queries, they aren’t a simple index scan. However, database reads are typically easier to scale than writes.

## How it Works

### Client View Records

A Client View Record (CVR) is a map of ID/version pairs. It captures the minimum information required about what was sent to a client in a previous pull. Encoded as JSON, it might look like:

```tsx
// key/version pairs
{
	"thread/3aasd39sa": 34,
	"comment/xskdjai": 18,
	"thread/33ska0a": 7,
	...
}
```

One CVR is generated for each pull response and stored in some ephemeral storage. The storage doesn’t need to be durable — if the CVR is lost, the server can just send a full sync. And the storage doesn’t need to be transactional with the database. Redis is perfect.

The CVRs are stored keyed under some unique ID. It can be a GUID, or a hash of the serialized CVR.

## Database Schema

The minimum requirement is just that each entity has a `version` column.

There is no need for soft deletes (`deleted` column) or spaces. Though they don’t hurt anything either.

## Push

- Process the push as normal
- Whenever any entity is updated, transactionally bump its `version` column. Note this is different than the way this column works in the Global Version strategy — each entity’s version is bumped independently on write, they aren’t all sharing one global or per-space version.

## Pull

- Use the request cookie to fetch the corresponding CVR from redis. If the request cookie is `null` or the CVR is not found in redis, let the CVR be `null`.
- Fetch **just the ids and versions** (not the values) of the client view from the database. This query can be any arbitrary function of the database.
- Build the **put-id-set**, a set of IDs that have been added or modified in the client view since the last pull, and therefore need to be sent to client:
  - all IDs in the CV that aren’t in the CVR
  - all IDs in both the CV and CVR whose version is greater in CV
- Build the **del-id-set**: a set of IDs that have been removed from the client view since last pull:
  - all IDs in CVR not in CV
- Update the CVR by adding all items in put-set to CVR and removing all items in del-set
- let _cvr-id_: newGUID()
- Store the cvr with cvr-id in redis
- Build **put-entities:** read full entities from db for all items in put-set
- Build the response patch and return response to client. The response cookie is the cvr-id.

## Queries and Windowing

The query that builds the client view can change at any time, and can even be per-user. However, slight care must be taken because of the way that Replicache data is shared between tabs. Changing the pull query in one tab changes it for other tabs that are sharing the same Replicache. Without coordination, this could result in two tabs “fighting” over the current query.

The solution is to sync the current query with Replicache (🤯). That way it will be automatically synced to all tabs.

- Add a new entity to the backend database to store the current query for a profile. Like other entities it should have a `version` field. Let’s say: `/control/<profile-id>/query`.
- When computing the pull, first read this value. If not present, use the default query. Include this entity in the pull response as any other entity.
- In the UI can use the query data in the client view to check and uncheck filter boxes, etc., just like other Replicache data!
- Add mutations that modify this entity.

## Variations

- The CVR can be passed into the database as an argument enabling the pull to be computed in a single DB round-trip.
- The CVR can be **\*\***stored**\*\*** in the primary database, allowing the pull to be computed with a single network round trip (no redis required). The downside is you must expire the CVR entries manually as you can’t rely on Redis caching.
- The per-row version number can also be a hash over the row serialization, or even a random GUID. These approaches might perform better in some datastores since it eliminates a read of the existing row during write.
- It is still totally fine to partition data into spaces and have a per-space `version` column ([aka a “semaphore”](https://dev.mysql.com/doc/refman/5.7/en/innodb-deadlocks-handling.html)) to enforce serialization and avoid deadlocks. This just becomes orthogonal to computing pulls.
