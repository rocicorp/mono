---
title: Remote Database
slug: /guide/remote-database
---

Replicache automatically batches mutations and sends them to the `replicache-push` endpoint periodically. Implementing the push handler is not much different than implementing a REST or GraphQL endpoint, with one key difference.

Each mutation is identified with a `mutationID` which is a per-client incrementing integer. The server must store this value transactionally when applying the mutation, and return it later in `replicache-pull`. This is what allows Replicache to know when speculative mutations have been confirmed by the server and thus no longer need to be replayed (and in fact can be discarded).

For this demo, we're using [Supabase](https://supabase.io), a very nice hosted Postgres database with a snazzy name, but Replicache can be used with most databases.

:::note Backend Requirements

Replicache is designed to work with your existing backend. You can use any datastore as long as it supports multikey atomic transactions, so that data changes and the corresponding update to `lastMutationID` happen at the same time.

For example, if the `lastMutationID` is 42, then the effects of all mutations <= 42 must be visible in `pull` endpoint responses, and the effects of > 42 must not be present.

Some examples of suitable datastores are: MySQL, Postgres, CockroachDB, CosmosDB, DynamoDB, and Firebase Cloud Firestore (but **not** Realtime Database).

:::

Head over to [Supabase](https://supabase.io) and create a free account and an empty database. Then add Supabase's PSQL connection string to your environment. You can get it from your Supabase project by clicking on ⚙️ (Gear/Cog) > Database > Connection String.

```bash
export REPLICHAT_DB_CONNECTION_STRING='<your connection string>'
```

Then, create a new file `db.js` with this code:

```js
import pgInit from 'pg-promise';

const pgp = pgInit();
export const db = pgp(process.env.REPLICHAT_DB_CONNECTION_STRING);
```

And another new file `pages/api/init.js` that initializes the schema:

```js
import {db} from '../../db.js';

export default async (_, res) => {
  await db.task(async t => {
    await t.none('DROP TABLE IF EXISTS message');
    await t.none('DROP TABLE IF EXISTS replicache_client');
    await t.none('DROP SEQUENCE IF EXISTS version');
    // Stores chat messages
    await t.none(`CREATE TABLE message (
      id VARCHAR(21) PRIMARY KEY NOT NULL,
      sender VARCHAR(255) NOT NULL,
      content TEXT NOT NULL,
      ord BIGINT NOT NULL,
      version BIGINT NOT NULL)`);
    // Stores last mutation ID for each Replicache client
    await t.none(`CREATE TABLE replicache_client (
      id VARCHAR(36) PRIMARY KEY NOT NULL,
      last_mutation_id BIGINT NOT NULL)`);
    // Will be used for computing diffs for pull response
    await t.none('CREATE SEQUENCE version');
  });
  res.send('ok');
};
```

Start up your server again and navigate to [http://localhost:3000/api/init](http://localhost:3000/api/init). You should see the text "OK" after a few moments. Then if you go to your Supabase UI, you should see the new tables.

<p class="text--center">
  <img src="/img/setup/schema-init.webp" width="650"/>
</p>
