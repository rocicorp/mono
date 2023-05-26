---
title: Remote Mutations
slug: /byob/remote-mutations
---

Replicache will periodically invoke push sending a list of mutations that need to be applied.

For each received mutation, the push handler must do several things:

1. Create a client record for the requesting client if the client is new.
1. Validate that the received mutation is the next expected one. If the received mutation has already been processed (by a previous push), skip it. If the received mutation is not expected, then error.
1. Increment the stored `version` for the affected space.
1. Run the received mutation by making the requested changes to the backend database. For any modified domain data objects, update their `version` to the new version for the space.
1. Update the stored `lastMutationID` for the pushing client, so that `pull` can later report the last-processed mutationID.

At minimum, all of these changes **must** happen atomically in a serialized transaction for each mutation in a push. However, putting multiple mutations together in a single wider transaction is also acceptable.

## Implement Push

Create a new file `pages/api/replicache-push.js` and copy the below code into it.

This looks like a lot of code, but it's just implementing the description above. See the inline comments for additional details.

```js
import {tx} from '../../db.js';
import Pusher from 'pusher';
import {defaultSpaceID} from './init.js';

export {handlePush as default};

async function handlePush(req, res) {
  const push = req.body;
  console.log('Processing push', JSON.stringify(push));

  const t0 = Date.now();
  try {
    // Iterate each mutation in the push.
    for (const mutation of push.mutations) {
      const t1 = Date.now();

      try {
        await tx(t =>
          processMutation(t, push.clientID, defaultSpaceID, mutation),
        );
      } catch (e) {
        console.error('Caught error from mutation', mutation, e);

        // Handle errors inside mutations by skipping and moving on. This is
        // convenient in development but you may want to reconsider as your app
        // gets close to production:
        //
        // https://doc.replicache.dev/server-push#error-handling
        //
        // Ideally we would run the mutator itself in a nested transaction, and
        // if that fails, rollback just the mutator and allow the lmid and
        // version updates to commit. However, nested transaction support in
        // Postgres is not great:
        //
        // https://postgres.ai/blog/20210831-postgresql-subtransactions-considered-harmful
        //
        // Instead we implement skipping of failed mutations by *re-runing*
        // them, but passing a flag that causes the mutator logic to be skipped.
        //
        // This ensures that the lmid and version bookkeeping works exactly the
        // same way as in the happy path. A way to look at this is that for the
        // error-case we replay the mutation but it just does something
        // different the second time.
        //
        // This is allowed in Replicache because mutators don't have to be
        // deterministic!:
        //
        // https://doc.replicache.dev/concepts/how-it-works#speculative-execution-and-confirmation
        await tx(t =>
          processMutation(t, push.clientID, defaultSpaceID, mutation, e),
        );
      }

      console.log('Processed mutation in', Date.now() - t1);
    }

    res.send('{}');

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

async function processMutation(t, clientID, spaceID, mutation, error) {
  // Get the previous version for the affected space and calculate the next
  // one.
  const {version: prevVersion} = await t.one(
    'select version from space where key = $1 for update',
    spaceID,
  );
  const nextVersion = prevVersion + 1;

  const lastMutationID = await getLastMutationID(t, clientID, false);
  const nextMutationID = lastMutationID + 1;

  console.log('nextVersion', nextVersion, 'nextMutationID', nextMutationID);

  // It's common due to connectivity issues for clients to send a
  // mutation which has already been processed. Skip these.
  if (mutation.id < nextMutationID) {
    console.log(
      `Mutation ${mutation.id} has already been processed - skipping`,
    );
    return;
  }

  // If the Replicache client is working correctly, this can never
  // happen. If it does there is nothing to do but return an error to
  // client and report a bug to Replicache.
  if (mutation.id > nextMutationID) {
    throw new Error(`Mutation ${mutation.id} is from the future - aborting`);
  }

  if (error === undefined) {
    console.log('Processing mutation:', JSON.stringify(mutation));

    // For each possible mutation, run the server-side logic to apply the
    // mutation.
    switch (mutation.name) {
      case 'createMessage':
        await createMessage(t, mutation.args, spaceID, nextVersion);
        break;
      default:
        throw new Error(`Unknown mutation: ${mutation.name}`);
    }
  } else {
    // TODO: You can store state here in the database to return to clients to
    // provide additional info about errors.
    console.log(
      'Handling error from mutation',
      JSON.stringify(mutation),
      error,
    );
  }

  console.log('setting', clientID, 'last_mutation_id to', nextMutationID);
  // Update lastMutationID for requesting client.
  await setLastMutationID(t, clientID, nextMutationID);

  // Update version for space.
  await t.none('update space set version = $1 where key = $2', [
    nextVersion,
    spaceID,
  ]);
}

export async function getLastMutationID(t, clientID, required) {
  const clientRow = await t.oneOrNone(
    'select last_mutation_id from replicache_client where id = $1',
    clientID,
  );
  if (!clientRow) {
    // If the client is unknown ensure the request is from a new client. If it
    // isn't, data has been deleted from the server, which isn't supported:
    // https://github.com/rocicorp/replicache/issues/1033.
    if (required) {
      throw new Error(`client not found: ${clientID}`);
    }
    return 0;
  }
  return parseInt(clientRow.last_mutation_id);
}

async function setLastMutationID(t, clientID, mutationID) {
  const result = await t.result(
    'update replicache_client set last_mutation_id = $2 where id = $1',
    [clientID, mutationID],
  );
  if (result.rowCount === 0) {
    await t.none(
      'insert into replicache_client (id, last_mutation_id) values ($1, $2)',
      [clientID, mutationID],
    );
  }
}

async function createMessage(t, {id, from, content, order}, spaceID, version) {
  await t.none(
    `insert into message (
    id, space_id, sender, content, ord, deleted, version) values
    ($1, $2, $3, $4, $5, false, $6)`,
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
