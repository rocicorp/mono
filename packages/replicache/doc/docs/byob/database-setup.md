---
title: Remote Database
slug: /byob/remote-database
---

Replicache is also backend-agnostic. You can use most backend languages and frameworks, and any backend datastore that supports at least [Snapshot Isolation](https://en.wikipedia.org/wiki/Snapshot_isolation).

Some examples of suitable datastores are: MySQL, Postgres, CockroachDB, CosmosDB, and Firebase Cloud Firestore. Some examples of non-suitable datastores are: DynamoDB and Firebase RealtimeDB.

:::info

Snapshot isolation is required for correct operation of Replicache. See [Database Isolation Level](/concepts/db-isolation-level) for more information.

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

export type Transaction = ITask<{}>;
type TransactionCallback<R> = (t: Transaction) => Promise<R>;

// In Postgres, snapshot isolation is known as "repeatable read".
export async function tx<R>(f: TransactionCallback<R>, dbp = getDB()) {
  const db = await dbp;
  return await db.tx(
    {
      mode: new txMode.TransactionMode({
        tiLevel: isolationLevel.repeatableRead,
      }),
    },
    f,
  );
}
```

## Next

In the [next section](./database-schema.md), we'll build our remote database schema.
