import type * as dag from './dag/mod.js';
import {getClientGroup} from './persist/client-groups.js';
import {withRead} from './with-transactions.js';

function makeChannelName(replicacheName: string): string {
  return `replicache-new-client-group:${replicacheName}`;
}

export {makeChannelName as makeChannelNameForTesting};

type NewClientChannelMessageV0 = [clientGroupID: string];
type NewClientChannelMessageV1 = [clientGroupID: string, idbName: string];

function istNewClientChannelMessageV0(
  message: unknown,
): message is NewClientChannelMessageV0 {
  return (
    Array.isArray(message) &&
    message.length === 1 &&
    typeof message[0] === 'string'
  );
}

function istNewClientChannelMessageV1(
  message: unknown,
): message is NewClientChannelMessageV1 {
  return (
    Array.isArray(message) &&
    message.length === 2 &&
    typeof message[0] === 'string' &&
    typeof message[1] === 'string'
  );
}

export function initNewClientChannel(
  replicacheName: string,
  idbName: string,
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
    channel.postMessage([clientGroupID, idbName]);
  }

  channel.onmessage = async (e: MessageEvent) => {
    const {data} = e;
    if (istNewClientChannelMessageV0(data)) {
      const [newClientGroupID] = data;
      if (newClientGroupID !== clientGroupID) {
        onUpdateNeeded();
      }
      return;
    }
    if (istNewClientChannelMessageV1(data)) {
      const [newClientGroupID, newClientIDBName] = data;
      if (newClientGroupID !== clientGroupID) {
        if (newClientIDBName === idbName) {
          // Check if this client can see the new client's newClientGroupID in its
          // perdag. It should be able to if the clients share persistent
          // storage. However, with `ReplicacheOption.experimentalCreateKVStore`
          // and `IDBStoreWithMemFallback` clients may not actually share
          // persistent storage.  If storage is not shared, then there is no point
          // in updating, since clients cannot sync locally.  If clients do update
          // in this case, they can continually cause each other to update, since
          // on each update the clients get assigned a new client group.
          const updateNeeded = await withRead(
            perdag,
            async (perdagRead: dag.Read) =>
              (await getClientGroup(newClientGroupID, perdagRead)) !==
              undefined,
          );
          if (updateNeeded) {
            onUpdateNeeded();
          }
        } else {
          // Idb name is different, indicating ew schema or format version.
          // Update to get assigned to newClientIDBName, and hopefully
          // newClientGroupID.
          // If storage is not actually shared (i.e. due to
          // `ReplicacheOption.experimentalCreateKVStore`
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

  signal.addEventListener('abort', () => channel.close());
}
