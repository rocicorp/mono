---
title: Remote Schema
slug: /byob/remote-schema
---

There are a number of ways to implement Replicache backends. The Replicache client doesn't actually care how your backend works internally — it only cares that you provide correctly implemented [push](/reference/server-push.md) and [pull](/reference/server-pull.md) endpoints.

However, in practice almost all Replicache backends end up using a similar pattern internally. Most need to track three kinds of persistent state:

- **Spaces:** Collections of data that are synced via the Replicache protocol. Each space has a _version_ that increments when the space is modified by a push. Your application's data is partioned into these spaces.
- **Clients:** Clients that have connected to the server, and the last mutationID processed from each. This is used during push to ensure mutations are processed only once, and in the order they happened on the client.
- **Domain Data:** The user data the application stores to do its job. Each stored item has a few extra Replicache-specific attributes:
  - `spaceID`: The `space` the domain object is part of.
  - `lastUpdatedVersion`: The `version` of the containing space that this item was last updated at. Used to calculate a diff during pull.
  - `deleted`: A [soft delete](https://en.wiktionary.org/wiki/soft_deletion) used to communicate to clients during pull that a item was logically deleted.

:::info

Spaces can be any size, subject to a few constraints:

- Clients don't have to pull the entire space, but the subset pulled to any one client is currently limited to about 64MB.
- Pushes against spaces are executed serially, so transactional throughput is limited by how fast your server can process pushes. For typical server setups, a good max estimate is about 50 pushes/second/space.

:::

## Define the Schema

Let's define a Postgres schema for the data model described above. And another new file at `pages/api/init.js`:

```js
import {tx} from '../../db.js';

// For this tutorial, we will use just one space. For a real application, you
// should partition your data into spaces as makes sense for your application.
export const defaultSpaceID = 'default';

export default async function init(_, res) {
  await tx(async t => {
    await t.none('drop table if exists replicache_client');
    await t.none('drop table if exists message');
    await t.none('drop table if exists space');

    // We will store our chat messages within "spaces".
    // Each space has a version that increments for each push processed.
    // Note that in many applications there is already some domain object that
    // already fills the role of a "space". In that case, that table can double
    // as the space table.
    await t.none(`create table space (
        key text not null unique primary key,
        version integer)`);
    await t.none(
      `insert into space (key, version) values ('${defaultSpaceID}', 0)`,
    );

    // Stores chat messages.
    await t.none(`create table message (
      id text primary key not null,
      space_id text not null references space(key),
      sender varchar(255) not null,
      content text not null,
      ord integer not null,
      deleted boolean not null,
      version integer not null)`);

    // Stores last mutationID processed for each Replicache client.
    await t.none(`create table replicache_client (
      id varchar(36) primary key not null,
      last_mutation_id integer not null)`);
  });
  res.send('ok');
}
```

Start up your server again and navigate to [http://localhost:3000/api/init](http://localhost:3000/api/init). You should see the text "OK" after a few moments. Then if you go to your Supabase UI, you should see the new tables.

<p class="text--center">
  <img src="/img/setup/schema-init.webp" width="650"/>
</p>

## Next

The [next section](./remote-mutations.md) implements remote mutations, so that our optimistic changes can become persitent.
