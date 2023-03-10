import type {ClientID, ClientMap} from '../types/client-state.js';
import type {PushBody} from 'reflect-protocol';
import type {LogContext} from '@rocicorp/logger';
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {closeWithError} from '../util/socket.js';
import {must} from '../util/must.js';
import {ErrorKind} from 'reflect-protocol';
import type {PendingMutation} from '../types/mutation.js';

export type Now = () => number;
export type ProcessUntilDone = () => void;

/**
 * handles the 'push' upstream message by queueing the mutations included in
 * [[body]] into pendingMutations.
 * O(p + m) where p is pendingMutations.length and m is body.mutations.length
 * (assuming array splice is O(1))
 */
export async function handlePush(
  lc: LogContext,
  storage: DurableStorage,
  clientID: ClientID,
  clients: ClientMap,
  pendingMutations: PendingMutation[],
  body: PushBody,
  now: Now,
  processUntilDone: ProcessUntilDone,
) {
  lc = lc.addContext('requestID', body.requestID);
  lc.debug?.('handling push', JSON.stringify(body));

  const client = must(clients.get(clientID));
  const clockOffsetMs = client.clockOffsetMs ?? now() - body.timestamp;
  if (client.clockOffsetMs === undefined) {
    client.clockOffsetMs = clockOffsetMs;
    lc.debug?.('initializing client clockOffsetMs to', client.clockOffsetMs);
  }

  const {clientGroupID} = body;
  const mutationClientIDs = new Set(body.mutations.map(m => m.clientID));
  const clientRecords = new Map(
    await Promise.all(
      [...mutationClientIDs].map(
        async mClientID =>
          [mClientID, await getClientRecord(mClientID, storage)] as [
            ClientID,
            ClientRecord | undefined,
          ],
      ),
    ),
  );
  const mutationIdRangesByClientID: Map<ClientID, [number, number]> = new Map();
  for (const {clientID, id} of body.mutations) {
    const range = mutationIdRangesByClientID.get(clientID);
    mutationIdRangesByClientID.set(clientID, range ? [range[0], id] : [id, id]);
  }

  const previousMutationByClientID: Map<
    ClientID,
    {id: number; pendingIndex: number}
  > = new Map();
  const newClientIDs: ClientID[] = [];
  for (const mClientID of mutationClientIDs) {
    const clientRecord = clientRecords.get(mClientID);
    previousMutationByClientID.set(mClientID, {
      id: clientRecord?.lastMutationID ?? 0,
      pendingIndex: -1,
    });
    if (clientRecord) {
      if (clientRecord.clientGroupID !== clientGroupID) {
        // This is not expected to ever occur.  However if it does no pushes
        // will ever succeed over this connection since the server and client
        // disagree about what client group a client id belongs to.  Even
        // after reconnecting this client is likely to be stuck.
        const errMsg = `Push for client ${clientID} with clientGroupID ${clientGroupID} contains mutation for client ${mClientID} which belongs to clientGroupID ${clientRecord.clientGroupID}.`;
        lc.error?.(errMsg);
        closeWithError(lc, client.socket, ErrorKind.InvalidPush, errMsg);
        return;
      }
    } else {
      newClientIDs.push(mClientID);
    }
  }

  let previousPushMutationIndex = -1;
  const pendingDuplicates: Map<string, number> = new Map();
  for (let i = 0; i < pendingMutations.length; i++) {
    const m = pendingMutations[i];
    if (m.pusherClientIDs.has(clientID)) {
      previousPushMutationIndex = i;
    }
    previousMutationByClientID.set(m.clientID, {id: m.id, pendingIndex: i});
    const range = mutationIdRangesByClientID.get(m.clientID);
    if (range && range[0] <= m.id && range[1] >= m.id) {
      pendingDuplicates.set(m.clientID + ':' + m.id, i);
    }
  }
  const inserts: [number, PendingMutation][] = [];
  for (const m of body.mutations) {
    const {id: previousMutationID, pendingIndex: previousPendingIndex} = must(
      previousMutationByClientID.get(m.clientID),
    );
    if (m.id <= previousMutationID) {
      const pendingDuplicateIndex = pendingDuplicates.get(
        m.clientID + ':' + m.id,
      );
      if (pendingDuplicateIndex !== undefined) {
        previousPushMutationIndex = Math.max(
          pendingDuplicateIndex,
          previousPushMutationIndex,
        );
      }
      continue;
    }
    if (m.id > previousMutationID + 1) {
      // No pushes will ever succeed over this connection since the client
      // is out of sync with the server. Close connection so client can try to
      // reconnect and recover.
      closeWithError(
        lc,
        client.socket,
        ErrorKind.InvalidPush,
        `Push contains unexpected mutation id ${m.id} for client ${
          m.clientID
        }. Expected mutation id ${previousMutationID + 1}.`,
      );
      return;
    }

    const normalizedTimestamp =
      m.clientID === clientID ? m.timestamp + clockOffsetMs : undefined;
    const mWithNormalizedTimestamp: PendingMutation = {
      ...m,
      clientGroupID,
      pusherClientIDs: new Set([clientID]),
      timestamp: normalizedTimestamp,
    };

    let insertIndex =
      Math.max(previousPushMutationIndex, previousPendingIndex) + 1;
    for (; insertIndex < pendingMutations.length; insertIndex++) {
      if (normalizedTimestamp === undefined) {
        break;
      }
      const pendingM = pendingMutations[insertIndex];
      if (
        pendingM.timestamp !== undefined &&
        pendingM.timestamp > normalizedTimestamp
      ) {
        break;
      }
    }

    previousPushMutationIndex = insertIndex;
    previousMutationByClientID.set(m.clientID, {
      id: m.id,
      pendingIndex: insertIndex,
    });
    inserts.push([insertIndex, mWithNormalizedTimestamp]);
  }
  await Promise.all(
    newClientIDs.map(clientID =>
      putClientRecord(
        clientID,
        {
          clientGroupID,
          baseCookie: null,
          lastMutationID: 0,
          lastMutationIDVersion: null,
        },
        storage,
      ),
    ),
  );

  for (const i of pendingDuplicates.values()) {
    const pendingM = pendingMutations[i];
    const pusherClientIDs = new Set(pendingM.pusherClientIDs);
    pusherClientIDs.add(clientID);
    pendingMutations[i] = {
      ...pendingM,
      pusherClientIDs,
    };
  }

  for (let i = 0; i < inserts.length; i++) {
    pendingMutations.splice(inserts[i][0] + i, 0, inserts[i][1]);
  }

  lc.debug?.(
    'inserted mutations, client group id',
    clientGroupID,
    'now has',
    pendingMutations.length,
    'pending mutations.',
  );
  processUntilDone();
}
