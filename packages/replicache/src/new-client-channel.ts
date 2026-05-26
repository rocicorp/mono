import {BroadcastChannel} from '../../shared/src/broadcast-channel.ts';
import type {Read, Store} from './dag/store.ts';
import {getClientGroup} from './persist/client-groups.ts';
import {withRead} from './with-transactions.ts';

// Older clients (<= replicache@13.0.1), listened on this channel name
// and *asserted* that the messages received were an array containing exactly
// one string.
function makeChannelNameV0(replicacheName: string): string {
  return `replicache-new-client-group:${replicacheName}`;
}

// This channel name was introduced when we first needed to change the message
// format.  The design of the messages sent on this channel allows for
// the message content to be extended in the future in a way that is
// forward and backwards compatible.  The message format can be extended
// by adding new *optional* fields.
function makeChannelNameV1(replicacheName: string): string {
  return `replicache-new-client-group-v1:${replicacheName}`;
}

export {
  makeChannelNameV0 as makeChannelNameV0ForTesting,
  makeChannelNameV1 as makeChannelNameV1ForTesting,
};

// This message type can be extended with optional properties.
type NewClientChannelMessageV1 = {clientGroupID: string; idbName: string};

function isNewClientChannelMessageV1(
  message: unknown,
): message is NewClientChannelMessageV1 {
  return (
    typeof message === 'object' &&
    typeof (message as {clientGroupID: unknown}).clientGroupID === 'string' &&
    typeof (message as {idbName: unknown}).idbName === 'string'
  );
}

export function initNewClientChannel(
  replicacheName: string,
  idbName: string,
  signal: AbortSignal,
  clientGroupID: string,
  isNewClientGroup: boolean,
  onUpdateNeeded: () => void,
  perdag: Store,
) {
  if (signal.aborted) {
    return;
  }

  const channelV1 = new BroadcastChannel(makeChannelNameV1(replicacheName));
  if (isNewClientGroup) {
    channelV1.postMessage({clientGroupID, idbName});
    // Send expected format to V0 channel for old clients.
    const channelV0 = new BroadcastChannel(makeChannelNameV0(replicacheName));
    channelV0.postMessage([clientGroupID]);
    channelV0.close();
  }

  channelV1.onmessage = async (e: MessageEvent) => {
    const {data} = e;
    if (isNewClientChannelMessageV1(data)) {
      const {clientGroupID: newClientGroupID, idbName: newClientIDBName} = data;
      if (newClientGroupID !== clientGroupID) {
        if (newClientIDBName === idbName) {
          // Check if this client can see the new client's newClientGroupID in its
          // perdag. It should be able to if the clients share persistent
          // storage. However, with `ReplicacheOption.kvStore`
          // and `IDBStoreWithMemFallback` clients may not actually share
          // persistent storage.  If storage is not shared, then there is no point
          // in updating, since clients cannot sync locally.  If clients do update
          // in this case, they can continually cause each other to update, since
          // on each update the clients get assigned a new client group.
          const updateNeeded = await withRead(
            perdag,
            async (perdagRead: Read) =>
              (await getClientGroup(newClientGroupID, perdagRead)) !==
              undefined,
          );
          if (updateNeeded) {
            onUpdateNeeded();
          }
        } else {
          // Idb name is different, indicating new schema or format version.
          // Update to get assigned to newClientIDBName, and hopefully
          // newClientGroupID.
          // If storage is not actually shared (i.e. due to
          // `ReplicacheOption.kvStore`
          // or `IDBStoreWithMemFallback`) the new client will not
          // get assigned to newClientGroupID, but should get the
          // newClientIDBName.
          // Note: we don't try to read from newClientIDBName to see
          // if this client shares storage with the new client, because
          // the newClientIDBName may have a format version this client
          // cannot read.
          onUpdateNeeded();
          return;
        }
      }
    }
  };

  signal.addEventListener('abort', () => channelV1.close(), {once: true});
}
