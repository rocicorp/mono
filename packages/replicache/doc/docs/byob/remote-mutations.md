---
title: Remote Mutations
slug: /byob/remote-mutations
---

Replicache will periodically invoke your [push endpoint](/reference/server-push) sending a list of mutations that need to be applied.

The implementation of push will depend on the backend strategy you are using. For the [Global Version](/strategies/global-version) strategy we're using, the basics steps are:

1. Open a transaction.
1. Read the current global version and compute the next one.
1. Create a client record for the requesting client if the client is new.
1. Validate that the received mutation is the next expected one. If the received mutation has already been processed (by a previous push), skip it. If the received mutation is not expected, then error.
1. Run the received mutation by making the requested changes to the backend database. For any modified domain data objects, update their `version` to the new global version.
1. Update the stored `version` and `lastMutationID` for the pushing client, so that `pull` can later report the last-processed mutationID.
1. Store the new global version.

At minimum, all of these changes **must** happen atomically in a single transaction for each mutation in a push. However, putting multiple mutations together in a single wider transaction is also acceptable.

## Implement Push

Create a new file `pages/api/replicache-push.ts` and copy the below code into it.

This looks like a lot of code, but it's just implementing the description above. See the inline comments for additional details.

```ts
import {NextApiRequest, NextApiResponse} from 'next';
import {serverID, tx, Transaction} from '../../db';
import Pusher from 'pusher';
import {MessageWithID} from '../../types';
import {MutationV1} from 'replicache';

export default async function (req: NextApiRequest, res: NextApiResponse) {
  const push = req.body;
  console.log('Processing push', JSON.stringify(push));

  const t0 = Date.now();
  try {
    // Iterate each mutation in the push.
    for (const mutation of push.mutations) {
      const t1 = Date.now();

      try {
        await tx(t => processMutation(t, push.clientGroupID, mutation));
      } catch (e) {
        console.error('Caught error from mutation', mutation, e);

        // Handle errors inside mutations by skipping and moving on. This is
        // convenient in development but you may want to reconsider as your app
        // gets close to production:
        // https://doc.replicache.dev/reference/server-push#error-handling
        await tx(t => processMutation(t, push.clientGroupID, mutation, e));
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

async function processMutation(
  t: Transaction,
  clientGroupID: string,
  mutation: MutationV1,
  error?: string | undefined,
) {
  const {clientID} = mutation;

  // Get the previous version and calculate the next one.
  const {version: prevVersion} = await t.one(
    'select version from replicache_server where id = $1 for update',
    serverID,
  );
  const nextVersion = prevVersion + 1;

  const lastMutationID = await getLastMutationID(t, clientID);
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
    throw new Error(
      `Mutation ${mutation.id} is from the future - aborting. This can happen in development if the server restarts. In that case, clear appliation data in browser and refresh.`,
    );
  }

  if (error === undefined) {
    console.log('Processing mutation:', JSON.stringify(mutation));

    // For each possible mutation, run the server-side logic to apply the
    // mutation.
    switch (mutation.name) {
      case 'createMessage':
        await createMessage(t, mutation.args as MessageWithID, nextVersion);
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
  await setLastMutationID(
    t,
    clientID,
    clientGroupID,
    nextMutationID,
    nextVersion,
  );

  // Update global version.
  await t.none('update replicache_server set version = $1 where id = $2', [
    nextVersion,
    serverID,
  ]);
}

export async function getLastMutationID(t: Transaction, clientID: string) {
  const clientRow = await t.oneOrNone(
    'select last_mutation_id from replicache_client where id = $1',
    clientID,
  );
  if (!clientRow) {
    return 0;
  }
  return parseInt(clientRow.last_mutation_id);
}

async function setLastMutationID(
  t: Transaction,
  clientID: string,
  clientGroupID: string,
  mutationID: number,
  version: number,
) {
  const result = await t.result(
    `update replicache_client set
      client_group_id = $2,
      last_mutation_id = $3,
      version = $4
    where id = $1`,
    [clientID, clientGroupID, mutationID, version],
  );
  if (result.rowCount === 0) {
    await t.none(
      `insert into replicache_client (
        id,
        client_group_id,
        last_mutation_id,
        version
      ) values ($1, $2, $3, $4)`,
      [clientID, clientGroupID, mutationID, version],
    );
  }
}

async function createMessage(
  t: Transaction,
  {id, from, content, order}: MessageWithID,
  version: number,
) {
  await t.none(
    `insert into message (
    id, sender, content, ord, deleted, version) values
    ($1, $2, $3, $4, false, $5)`,
    [id, from, content, order, version],
  );
}

async function sendPoke() {
  // TODO
}
```

:::info

You may be wondering if it possible to share mutator code between the client and server. It is, but constrains how you can design your backend. See [Share Mutators](/howto/share-mutators) for more information.

:::

Restart the server, navigate to [http://localhost:3000/](http://localhost:3000/) and make some changes. You should now see changes getting saved in the server console output.

<p class="text--center">
  <img src="/img/setup/remote-mutation.webp" width="650"/>
</p>

But if we check another browser, or an incognito window, the change isn't there. What gives?

## Next

In the next section, we implement [Dynamic Pull](./dynamic-pull.md) to propagate changes between users.
