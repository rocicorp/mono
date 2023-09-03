import {assert, assertArray, assertString} from 'shared/src/asserts.js';
import type * as dag from './dag/mod.js';
import {getClientGroup} from './persist/client-groups.js';
import {withRead} from './with-transactions.js';

function makeChannelName(replicacheName: string): string {
  return `replicache-new-client-group:${replicacheName}`;
}

export {makeChannelName as makeChannelNameForTesting};

type NewClientChannelMessage = [clientGroupID: string];

function assertNewClientChannelMessage(
  message: unknown,
): asserts message is NewClientChannelMessage {
  assertArray(message);
  assert(message.length === 1);
  assertString(message[0]);
}

export function initNewClientChannel(
  replicacheName: string,
  signal: AbortSignal,
  clientGroupID: string,
  isNewClientGroup: boolean,
  onUpdateNeeded: () => void,
  perdag: dag.Store,
) {
  if (signal.aborted) {
    return;
  }

  const channel = new BroadcastChannel(makeChannelName(replicacheName));
  if (isNewClientGroup) {
    channel.postMessage([clientGroupID]);
  }

  channel.onmessage = async (e: MessageEvent<NewClientChannelMessage>) => {
    const {data} = e;
    // Don't trust the message.
    assertNewClientChannelMessage(data);

    const [newClientGroupID] = data;
    if (newClientGroupID !== clientGroupID) {
      // Check if this client can see the new client's newClientGroupID in its
      // perdag.  It should be able t o if the clients share persistent storage.
      // However, with `ReplicacheOption.experimentalCreateKVStore`
      // clients may not actually share persistent storage.  If storage is not
      // shared, then there is no point in updating, since clients cannot
      // sync locally via client group.  If we did update in this case, we
      // would end up with the two clients continually causing each other to
      // update, since on each update the clients would get assigned
      // a new client group.
      const updateNeeded = await withRead(
        perdag,
        async (perdagRead: dag.Read) =>
          (await getClientGroup(newClientGroupID, perdagRead)) !== undefined,
      );
      if (updateNeeded) {
        onUpdateNeeded();
      }
    }
  };

  signal.addEventListener('abort', () => channel.close());
}
