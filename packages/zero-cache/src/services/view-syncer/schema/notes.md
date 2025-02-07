How to order the queries?

To achieve LRU and TTL we need the queries to be ordered by the time they were last accessed.

```sql
CREATE TABLE queries (
  ...
  last_accessed TIMESTAMP,
)
```

a query can be active or inactive. If it is active it will not be removed.

```sql
CREATE TABLE queries (
  ...
  "lastAccessed" TIMESTAMP,
  "active" BOOLEAN NOT NULL,,
)
```

An active query will never be removed, we can remove "lastAccessed" from the table.

```sql
CREATE TABLE queries (
  ...
  "active" BOOLEAN NOT NULL,,
)
```

When the query becomes inactive we want to keep it as long as there is space and it has not expired. So when we inactive it we set the inactivated time

```sql
CREATE TABLE queries (
  ...
  "active" BOOLEAN NOT NULL,
  "ttl" INTEGER,
  "inactivatedAt" TIMESTAMP,
)
```

We can use the "inactiveTime" `NULL` to signify an active query:

```sql
CREATE TABLE queries (
  ...
  "ttl" INTEGER,
  "inactivatedAt" TIMESTAMP,
)
```

When we need to remove a query we want to:

- Remove the oldest inactive query with an expire time
- Remove the oldest inactive query without an expire time
- Never remove an active query.

The expire time is computed as the `inactivatedAt + ttl`. We add a column for it even though it is is denormalized. We do this to add an index on "expiresAt".
(Maybe we do not need that since we are operating on the CVR in memory... we will see.)

```sql
CREATE TABLE queries (
  ...
  "ttl" INTEGER,
  "inactivatedAt" TIMESTAMP,
  "expiresAt" TIMESTAMP,
);

CREATE INDEX queries_index_expires_at ON queries ("expiresAt" NULLS LAST);
CREATE INDEX queries_index_inactivated_at ON queries ("inactivatedAt" NULLS LAST);

```

If we have not yet been inactivated expiresAt is `NULL`.
If we have no ttl expiresAt is also `NULL`.
This way we can order by `expiresAt` and then by `inactivatedAt` to remove the oldest query.

```sql
SELECT * FROM queries WHERE "inactivatedAt" IS NOT NULL ORDER BY "expiresAt" ASC, "inactivatedAt" ASC
```

Now we can start from the beginning and remove queries until we have enough space.
