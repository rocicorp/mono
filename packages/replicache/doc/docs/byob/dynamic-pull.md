---
title: Dynamic Pull
slug: /byob/dynamic-pull
---

Even though in the previous step we're making persistent changes in the database, we still aren't _serving_ that data in the pull endpoint (it's still static 🤣). The pull response is still static. Let's fix that now.

The pull endpoint takes as input:

- `clientGroupID`: The client group (roughly, the browser profile) that is making the request.
- `cookie`: The cookie the client group received from the previous pull, if any.

And it returns:

- `patch`: A set of changes (puts and deletes) that have occurred since the last pull.
- `cookie`: An opaque value that identifies the state the patch was calculated from. This value is sent back to the server on the next pull, so the next patch can be calculated.
- `lastMutationIDChanges`: A map with an entry for each client in the group whose `lastMutationID` has changed since the last pull.

See [pull endpoint reference](/reference/server-pull) for more details.

The implementation of pull will depend on the backend strategy you are using. For the [Global Version](/concepts/strategies/global-version) strategy we're using, the basics steps are:

<ul>
  <li>Open an exclusive (serializable) transaction</li>
  <li>Read the latest global version from the database</li>
  <li>Build the response patch:
    <ul>
      <li>If the request cookie is null, this patch contains a `put` for each entity in the database that isn't deleted</li>
      <li>Otherwise, this patch contains only entries that have been changed since the request cookie</li>
    </ul>
  </li>
  <li>Build a map of changes to client `lastMutationID` values:
    <ul>
      <li>If the request cookie is null, this map contains an entry for every client in the requesting `clientGroup`</li>
      <li>Otherwise, it contains only entries for clients that have changed since the request cookie</li>
    </ul>
  </li>
  <li>Return the patch, the current global `version`, and the `lastMutationID` changes as a `PullResponse` struct</li>
</ul>

## Implement Pull

Replace the contents of `pages/api/replicache-pull.js` with this code:

```js
import {tx} from '../../db.js';

export {handlePull as default};

async function handlePull(req, res) {
  const pull = req.body;
  console.log(`Processing pull`, JSON.stringify(pull));
  const t0 = Date.now();

  try {
    // Read all data in a single transaction so it's consistent.
    await tx(async t => {
      // Get current version.
      const version =
        (await t.one('select version from replicache_version')).version ?? 0;

      // Get lmid for requesting client.
      const isExistingClient = pull.lastMutationID > 0;
      const lastMutationIDChanges = await getLastMutationIDChanges(
        t,
        pull.clientGroupID,
        version,
      );
      // TODO: Deleted client check
      // Requires clientID in PullRequest

      // Get changed domain objects since requested version.
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
        lastMutationIDChanges,
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

async function getLastMutationIDChanges(t, clientGroupID, fromVersion) {
  const rows = await t.many(
    `select id, last_mutation_id
    from replicache_client
    where clientGroupID = $1 and version > $2`,
    clientGroupID,
    fromVersion,
  );
  return Object.fromEntries(rows.map(r => [r.id, r.last_mutation_id]));
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
