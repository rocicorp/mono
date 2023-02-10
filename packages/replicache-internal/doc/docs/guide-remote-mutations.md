---
title: Remote Mutations
slug: /guide/remote-mutations
---

Now to actually implement our push handler. Create a new file `pages/api/replicache-push.js` and copy the following code into it:

```js
import {db} from '../../db.js';

export default async (req, res) => {
  const push = req.body;
  console.log('Processing push', JSON.stringify(push));

  const t0 = Date.now();
  try {
    await db.tx(async t => {
      const {nextval: version} = await t.one("SELECT nextval('version')");
      let lastMutationID = await getLastMutationID(t, push.clientID);

      console.log('version', version, 'lastMutationID:', lastMutationID);

      for (const mutation of push.mutations) {
        const t1 = Date.now();

        const expectedMutationID = lastMutationID + 1;

        if (mutation.id < expectedMutationID) {
          console.log(
            `Mutation ${mutation.id} has already been processed - skipping`,
          );
          continue;
        }
        if (mutation.id > expectedMutationID) {
          console.warn(`Mutation ${mutation.id} is from the future - aborting`);
          break;
        }

        console.log('Processing mutation:', JSON.stringify(mutation));

        switch (mutation.name) {
          case 'createMessage':
            await createMessage(t, mutation.args, version);
            break;
          default:
            throw new Error(`Unknown mutation: ${mutation.name}`);
        }

        lastMutationID = expectedMutationID;
        console.log('Processed mutation in', Date.now() - t1);
      }

      console.log(
        'setting',
        push.clientID,
        'last_mutation_id to',
        lastMutationID,
      );
      await t.none(
        'UPDATE replicache_client SET last_mutation_id = $2 WHERE id = $1',
        [push.clientID, lastMutationID],
      );
      res.send('{}');
    });

    // We need to await here otherwise, Next.js will frequently kill the request
    // and the poke won't get sent.
    await sendPoke();
  } catch (e) {
    console.error(e);
    res.status(500).send(e.toString());
  } finally {
    console.log('Processed push in', Date.now() - t0);
  }
};

async function getLastMutationID(t, clientID) {
  const clientRow = await t.oneOrNone(
    'SELECT last_mutation_id FROM replicache_client WHERE id = $1',
    clientID,
  );
  if (clientRow) {
    return parseInt(clientRow.last_mutation_id);
  }

  console.log('Creating new client', clientID);
  await t.none(
    'INSERT INTO replicache_client (id, last_mutation_id) VALUES ($1, 0)',
    clientID,
  );
  return 0;
}

async function createMessage(t, {id, from, content, order}, version) {
  await t.none(
    `INSERT INTO message (
    id, sender, content, ord, version) values 
    ($1, $2, $3, $4, $5)`,
    [id, from, content, order, version],
  );
}

async function sendPoke() {
  // TODO
}
```

See [Push Endpoint Reference](../server-push) for complete details on implementing the push endpoint.

:::note info

In the code sample above, we updated a special `version` column in the message table which will be used in combination with Replicache's _cookie_ support to compute delta responses for `replicache-pull` in the next step. This is an easy way to do it and works for many apps. See [Cookie Monster Manual](#TODO) for other options.

:::

Restart the server, navigate to [http://localhost:3000/](http://localhost:3000/) and make some changes. You should now see changes getting saved in Supabase. Niiiice.

<p class="text--center">
  <img src="/img/setup/remote-mutation.webp" width="650"/>
</p>

But we don't see the change propagating to other browsers yet. What gives?
