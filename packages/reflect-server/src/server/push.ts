import type {ClientID, ClientState} from '../types/client-state.js';
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
 */
export async function handlePush(
  lc: LogContext,
  storage: DurableStorage,
  clientID: ClientID,
  client: ClientState,
  pendingMutations: PendingMutation[],
  body: PushBody,
  now: Now,
  processUntilDone: ProcessUntilDone,
) {
  lc = lc.addContext('requestID', body.requestID);
  lc.debug?.('handling push', JSON.stringify(body));

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
  for (let i = 0; i < pendingMutations.length; i++) {
    const m = pendingMutations[i];
    previousMutationByClientID.set(m.clientID, {id: m.id, pendingIndex: i});
  }
  let previousPushMutationIndex = -1;
  const newPendingMutations = [...pendingMutations];
  for (const m of body.mutations) {
    const {id: previousMutationID, pendingIndex: previousPendingIndex} = must(
      previousMutationByClientID.get(m.clientID),
    );
    if (m.id <= previousMutationID) {
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
    const mWithNormalizedTimestamp = {
      ...m,
      timestamp: normalizedTimestamp,
    };

    let insertIndex =
      Math.max(previousPushMutationIndex, previousPendingIndex) + 1;
    for (; insertIndex < newPendingMutations.length; insertIndex++) {
      if (normalizedTimestamp === undefined) {
        break;
      }
      const pendingM = newPendingMutations[insertIndex];
      if (
        pendingM.timestamp !== undefined &&
        pendingM.timestamp > normalizedTimestamp
      ) {
        break;
      }
    }

    previousPushMutationIndex = insertIndex;
    for (const [clientID, {id, pendingIndex}] of previousMutationByClientID) {
      if (m.clientID === clientID) {
        previousMutationByClientID.set(m.clientID, {
          id: m.id,
          pendingIndex: insertIndex,
        });
      } else {
        if (pendingIndex >= insertIndex) {
          previousMutationByClientID.set(m.clientID, {
            id,
            pendingIndex: pendingIndex + 1,
          });
        }
      }
    }
    pendingMutations.splice(insertIndex, 0, mWithNormalizedTimestamp);
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

  lc.debug?.(
    'inserted mutations, client group id',
    clientGroupID,
    'now has',
    pendingMutations.length,
    'pending mutations.',
  );
  processUntilDone();
}
