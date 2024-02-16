---
title: Remote Database
slug: /byob/remote-database
---

Replicache is also backend-agnostic. You can use most backend languages and frameworks, and any backend datastore that supports [serializable transactions](https://en.wikipedia.org/wiki/Serializability).

Some examples of suitable datastores are: MySQL, Postgres, CockroachDB, CosmosDB, and Firebase Cloud Firestore. Some examples of non-suitable datastores are: DynamoDB and Firebase RealtimeDB.

:::info

Serializable transactions are needed so that user data changes and the corresponding update to Replicache sync metadata happen together atomically.

For example, if a client's `lastMutationID` is 42, then the effects of all mutations <= 42 from that client must be visible in `pull` endpoint responses, and the effects of > 42 must not be present.

If this invariant is violated by the server then Replicache may not function properly and the UI of your application may exhibit weird behavior, such as duplicate or missing mutations.

:::

## Database Setup

For this demo, we'll use [pg-mem](https://github.com/oguimbal/pg-mem) — an in-memory implementation of Postgres. This is a nice easy way to play locally, but you can easily adapt this sample to use a remote Postgres implementation like [Render](https://render.com/) or [Supabase](https://supabase.com/).

Create a new file `db.ts` with this code:

```ts
import {newDb} from 'pg-mem';
import pgp, {IDatabase, ITask, txMode} from 'pg-promise';

const {isolationLevel} = pgp.txMode;

export const serverID = 1;

async function initDB() {
  console.log('initializing database...');
  const db = newDb().adapters.createPgPromise();
  return db;
}

function getDB() {
  // Cache the database in the Node global so that it survives HMR.
  if (!global.__db) {
    global.__db = initDB();
  }
  return global.__db as IDatabase<{}>;
}

// Helper to make sure we always access database at serializable level.
export async function tx<R>(f: (t: ITask<{}>) => Promise<R>, dbp = getDB()) {
  const db = await dbp;
  return await db.tx(
    {
      mode: new txMode.TransactionMode({
        tiLevel: isolationLevel.serializable,
      }),
    },
    f,
  );
}
```

## Next

In the [next section](./database-schema.md), we'll build our remote database schema.
