---
title: Remote Mutations
slug: /byob/remote-mutations
---

Replicache will periodically invoke push sending a list of mutations that need to be applied.

For each received mutation, the push handler must do four things:

1. Validate that the received mutation is the next expected one. If the received mutation has already been processed (by a previous push), skip it. If the received mutation is not expected, then error.
2. Increment the stored `version` for the affected space.
3. Run the received mutation by making the requested changes to the backend database. For any modified domain data objects, update their `version` to the new version for the space.
4. Update the stored `lastMutationID` for the pushing client, so that `pull` can later report the last-processed mutationID.

At minimum all four of these changes **must** happen atomically in a serialized transaction for each mutation in a push. However it's more common for entire pushes to be processed in a single transaction since it's often faster.

## Implement Push

Create a new file `pages/api/replicache-push.js` and copy the below code into it.

This looks like a lot of code, but it's just implementing the description above. See the inline comments for additional details.

```js
import {tx} from '../../db.js';
import Pusher from 'pusher';
import {defaultSpaceID} from './init.js';

export default handlePush;

async function handlePush(req, res) {
  const push = req.body;
  console.log('Processing push', JSON.stringify(push));

  const t0 = Date.now();
  try {
    // Run the entire push in one transaction.
    await tx(async t => {
      // Get the previous version for the affected space and calculate the next
      // one.
      const {version: prevVersion} = await t.one(
        'select version from space where key = $1',
        defaultSpaceID,
      );
      const nextVersion = prevVersion + 1;

      // Get the lastMutationID for the sending client, so we can know what the
      // expected next mutationID is.
      let lastMutationID = await getLastMutationID(t, push.clientID);

      console.log(
        'nextVersion',
        nextVersion,
        'lastMutationID:',
        lastMutationID,
      );

      // Iterate each mutation in the push.
      for (const mutation of push.mutations) {
        const t1 = Date.now();

        // Calculate the expected next mutationID.
        const expectedMutationID = lastMutationID + 1;

        // It's common due to connectivity issues for clients to send a
        // mutation which has already been processed. Skip these.
        if (mutation.id < expectedMutationID) {
          console.log(
            `Mutation ${mutation.id} has already been processed - skipping`,
          );
          continue;
        }

        // If the Replicache client is working correctly, this can never
        // happen. If it does there is nothing to do but return an error to
        // client and report a bug to Replicache.
        if (mutation.id > expectedMutationID) {
          console.warn(`Mutation ${mutation.id} is from the future - aborting`);
          break;
        }

        console.log('Processing mutation:', JSON.stringify(mutation));

        // For each possible mutation, run the server-side logic to apply the
        // mutation.
        try {
          switch (mutation.name) {
            case 'createMessage':
              await createMessage(
                t,
                mutation.args,
                defaultSpaceID,
                nextVersion,
              );
              break;
            default:
              throw new Error(`Unknown mutation: ${mutation.name}`);
          }
        } catch (e) {
          // Unhandled errors from mutations are discouraged. It is hard to
          // know whether the error is temporary (and would be resolved if we
          // retry the mutation later) or permanent (and would thus block that
          // client forever). We recommend to bias toward skipping such
          // mutations and avoiding blocking the client from progressing.
          console.error('Error processing mutation - skipping', e);
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

      // Update lastMutationID for requesting client.
      await t.none(
        'update replicache_client set last_mutation_id = $2 where id = $1',
        [push.clientID, lastMutationID],
      );

      // Update version for space.
      await t.none('update space set version = $1 where key = $2', [
        nextVersion,
        defaultSpaceID,
      ]);

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
}

async function getLastMutationID(t, clientID) {
  const clientRow = await t.oneOrNone(
    'select last_mutation_id from replicache_client where id = $1',
    clientID,
  );
  if (clientRow) {
    return parseInt(clientRow.last_mutation_id);
  }

  console.log('Creating new client', clientID);
  await t.none(
    'insert into replicache_client (id, last_mutation_id) values ($1, 0)',
    clientID,
  );
  return 0;
}

async function createMessage(t, {id, from, content, order}, spaceID, version) {
  await t.none(
    `insert into message (
    id, space_id, sender, content, ord, version) values
    ($1, $2, $3, $4, $5, $6)`,
    [id, spaceID, from, content, order, version],
  );
}

async function sendPoke() {
  // TODO
}
```

See [Push Endpoint Reference](/reference/server-push) for complete details on implementing the push endpoint.

:::info

You may be wondering if it possible to share mutator code between the client and server. It is, but constrains how you can design your backend. See [Share Mutators](/howto/share-mutators) for more information.

:::

Restart the server, navigate to [http://localhost:3000/](http://localhost:3000/) and make some changes. You should now see changes getting saved in Supabase. Niiiice.

<p class="text--center">
  <img src="/img/setup/remote-mutation.webp" width="650"/>
</p>

But we don't see the change propagating to other browsers yet. What gives?

## Next

In the next section, we implement [Dynamic Pull](./dynamic-pull.md) to see the result of these mutations.
