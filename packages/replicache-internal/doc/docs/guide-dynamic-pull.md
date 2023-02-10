---
title: Dynamic Pull
slug: /guide/dynamic-pull
---

Even though in the previous step we're making persistent changes in the remote database, we still aren't _serving_ that data in the pull endpoint (it's still static 🤣).

The reason we don't see the changes disappearing from the source browser is because Replicache is doing its job and holding onto speculative changes until they are confirmed by the server!

Let's fix that now. We'll also use the `cookie` field to return only changed messages.

Replace the contents of `pages/api/replicache-pull.js` with this code:

```js
import {db} from '../../db.js';

export default async (req, res) => {
  const pull = req.body;
  console.log(`Processing pull`, JSON.stringify(pull));
  const t0 = Date.now();

  try {
    await db.tx(async t => {
      const lastMutationID = parseInt(
        (
          await t.oneOrNone(
            'select last_mutation_id from replicache_client where id = $1',
            pull.clientID,
          )
        )?.last_mutation_id ?? '0',
      );
      const changed = await t.manyOrNone(
        'select id, sender, content, ord from message where version > $1',
        parseInt(pull.cookie ?? 0),
      );
      const cookie = (
        await t.one('select max(version) as version from message')
      ).version;
      console.log({cookie, lastMutationID, changed});

      const patch = [];
      if (pull.cookie === null) {
        patch.push({
          op: 'clear',
        });
      }

      patch.push(
        ...changed.map(row => ({
          op: 'put',
          key: `message/${row.id}`,
          value: {
            from: row.sender,
            content: row.content,
            order: parseInt(row.ord),
          },
        })),
      );

      res.json({
        lastMutationID,
        cookie,
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
};
```

Voila. We're now round-tripping browsers and devices!

<p class="text--center">
  <img src="/img/setup/manual-sync.webp" width="650"/>
</p>

Also notice that if we go offline for awhile, make some changes, then come back online, the mutations get sent when possible.

We don't have any conflicts in this simple data model, but Replicache makes it easy to reason about most conflicts. See the [Design Doc](/design) for more details.

The only thing left is to make it live — we obviously don't want the user to have to manually refresh to get new data 🙄.
