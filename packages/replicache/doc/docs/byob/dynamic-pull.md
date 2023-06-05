---
title: Dynamic Pull
slug: /byob/dynamic-pull
---

Even though in the previous step we're making persistent changes in the database, we still aren't _serving_ that data in the pull endpoint (it's still static 🤣).

Referring back to the [Global Version](/concepts/diff/global-version) doc again, the basic steps for pull are:

<ul>
  <li>Open an exclusive (serializable) transaction</li>
  <li>Read the current value of the global version</li>
  <li>Read the current value of the requesting client's lastMutationID</li>
  <li>If the request cookie is `null`:
    <ul>
      <li>Read all entities that where `IsDeleted=False`</li>
      <li>Create a _reset patch_ - a patch with a `clear` op followed by `put` ops for each read entity</li>
    </ul>
  </li>
  <li>Otherwise:
    <ul>
      <li>Read all entities whose `Version` is greater than the cookie value</li>
      <li>Create a patch having `del` ops for each entity where `IsDeleted=True`, and `put` ops for other entities</li>
    </ul>
  </li>
  <li>Return the calculated patch, the current value of the `GlobalVersion` field. and the requesting client's current `lastMutationID` as the pull response.</li>
</ul>

## Implement Pull

Replace the contents of `pages/api/replicache-pull.js` with this code:

```js
import {tx} from '../../db.js';
import {getLastMutationID} from './replicache-push.js';

export {handlePull as default};

async function handlePull(req, res) {
  const pull = req.body;
  console.log(`Processing pull`, JSON.stringify(pull));
  const t0 = Date.now();

  try {
    // Read all data in a single transaction so it's consistent.
    await tx(async t => {
      // Get current version.
      const version = (await t.one('select version from replicache_version'))
        .version;

      // Get lmid for requesting client.
      const isExistingClient = pull.lastMutationID > 0;
      const lastMutationID = await getLastMutationID(
        t,
        pull.clientID,
        isExistingClient,
      );

      // Get changed domain objects since requested version.
      const fromVersion = pull.cookie ?? 0;
      const changed = await t.manyOrNone(
        'select id, sender, content, ord, deleted from message where version > $1',
        fromVersion,
      );

      // Build and return response.
      const patch = [];
      for (const row of changed) {
        if (row.deleted) {
          if (fromVersion > 0) {
            patch.push({
              op: 'del',
              key: `message/${row.id}`,
            });
          }
        } else {
          patch.push({
            op: 'put',
            key: `message/${row.id}`,
            value: {
              from: row.sender,
              content: row.content,
              order: parseInt(row.ord),
            },
          });
        }
      }

      res.json({
        lastMutationID,
        cookie: version,
        patch,
      });
      res.end();
    });
  } catch (e) {
    console.error(e);
    res.status(500).send(e.toString());
  } finally {
    console.log('Processed pull in', Date.now() - t0);
  }
}
```

Voila. We're now round-tripping browsers and devices!

<p class="text--center">
  <img src="/img/setup/manual-sync.webp" width="650"/>
</p>

Also notice that if we go offline for awhile, make some changes, then come back online, the mutations get sent when possible.

We don't have any conflicts in this simple data model, but Replicache makes it easy to reason about most conflicts. See the [How Replicache Works](/concepts/how-it-works) for more details.

The only thing left is to make it live — we obviously don't want the user to have to manually refresh to get new data 🙄.

## Next

The [next section](./poke.md) implements realtime updates.
