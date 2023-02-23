---
title: Remote Database
slug: /byob/remote-database
---

Replicache is also backend-agnostic. You can use most backend languages and frameworks, and any backend datastore that supports [serializable transactions](https://en.wikipedia.org/wiki/Serializability).

Some examples of suitable datastores are: MySQL, Postgres, CockroachDB, CosmosDB, DynamoDB, and Firebase Cloud Firestore (but **not** Realtime Database).

:::info

Serializable transactions are needed so that user data changes and the corresponding update to Replicache sync metadata happen together atomically.

For example, if a client's `lastMutationID` is 42, then the effects of all mutations <= 42 from that client must be visible in `pull` endpoint responses, and the effects of > 42 must not be present.

If this invariant is violated by the server then Replicache may not function properly and the UI of your application may exhibit weird behavior, such as duplicate or missing mutations.

:::

## Supabase Setup

For this demo, we'll use [Supabase](https://supabase.io/) — a nice hosted Postgres service. Head over to [supabase.io](https://supabase.io) and create a free account and an empty database.

:::caution

Make sure to take note of your database password when you create your Supabase instance. You need it to construct your connection string in the next step, and it can't be retrieved after creation!

:::

Then add Supabase's PSQL connection string to your environment. You can get it from your Supabase project by clicking on ⚙️ (Gear/Cog) > Database > Connection String.

```bash
export REPLICHAT_DB_CONNECTION_STRING='<your connection string>'
```

Finally, create a new file `db.js` with this code:

```js
import pgInit from 'pg-promise';

const pgp = pgInit();
export const db = pgp(process.env.REPLICHAT_DB_CONNECTION_STRING);

const {isolationLevel} = pgp.txMode;

// Helper to make sure we always access database at serializable level.
export async function tx(f) {
  return await db.tx({mode: isolationLevel.serializable}, f);
}
```

## Next

In the [next section](./database-schema.md), we'll build our remote database schema.
