---
title: Dynamic Pull
slug: /byob/dynamic-pull
---

Even though in the previous step we're making persistent changes in the remote database, we still aren't _serving_ that data in the pull endpoint (it's still static 🤣).

Let's fix that now. [Pull](/reference/server-pull.md) needs to return three things:

1. A _cookie_ that identifies the current state of the requested space. We use the space's `version` for this purpose.
1. All domain objects that have changed since the last pull, formatted as [JSON Patch](https://jsonpatch.com/). This is easy to do because on each pull request includes the `cookie` the client last received. All we have to do is find domain objects with a bigger `version` than this value.
1. The last-processed `mutationID` for the calling client. This is how the client knows which mutations have been processed authoritatively and can therefore have their optimistic versions dropped.

Replace the contents of `pages/api/replicache-pull.js` with this code:

```js
import {tx} from '../../db.js';
import {defaultSpaceID} from './init.js';
import {getLastMutationID} from './replicache-push.js';

export default handlePull;

async function handlePull(req, res) {
  const pull = req.body;
  console.log(`Processing pull`, JSON.stringify(pull));
  const t0 = Date.now();

  try {
    // Read all data in a single transaction so it's consistent.
    await tx(async t => {
      // Get current version for space.
      const version = (
        await t.one('select version from space where key = $1', defaultSpaceID)
      ).version;

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
