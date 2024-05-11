import type {LogContext} from '@rocicorp/logger';
import type {PushBody} from 'reflect-protocol';
import {must} from 'shared/src/must.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {
  IncludeDeleted,
  getClientRecords,
  putClientRecord,
} from '../types/client-record.js';
import type {ClientID, ClientMap} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {closeWithError} from 'shared/src/cf/socket.js';

export type Now = () => number;
export type ProcessUntilDone = () => void;

const RESET_CLOCK_OFFSET_THRESHOLD_MS = 1000;
const OLD_MUTATION_THRESHOLD_MS = 50;

/**
 * handles the 'push' upstream message by queueing the mutations included in
 * [[body]] into pendingMutations.
 *
 * It ensures the following ordering constraints for mutations:
 * 1. mutation ids for a given client id must be in ascending order (without
 *    gaps)
 * 2. mutations from the same client group pushed by the same client, must be in
 *    the order they were pushed by that client (even across pushes), unless
 *    they were already pushed by a different client, then the already
 *    established order should persist.  This is important for dd31, as it
 *    ensures that if a mutation C1M1 was initially created on client C1
 *    after C1 refreshed a mutation C2M1 created by client C2 from their
 *    shared perdag, that C1M1 is ordered after C2M1, which is important
 *    since C1M1 may have a data dependency on C2M1 (e.g. C2M1 creates shape
 *    S1, and C1M1 changes the color of shape S1).
 * 3. mutations should be ordered by normalized timestamp, as long as it does
 *    not violate any of the above ordering constraints.  This will minimize
 *    forcing clients to miss frames when playing back the mutations.
 *
 * Runtime: O(p + m) where p is pendingMutations.length and m is
 * body.mutations.length
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
): Promise<void> {
  lc = lc.withContext('requestID', body.requestID);
  lc.debug?.('handling push');

  const client = must(clients.get(clientID));
  const timestamp = now();
  const pushClockOffsetMs = timestamp - body.timestamp;
  let {clockOffsetMs} = client;
  if (clockOffsetMs === undefined) {
    lc.debug?.('initializing client clockOffsetMs to', pushClockOffsetMs);
    clockOffsetMs = pushClockOffsetMs;
  } else if (
    Math.abs(clockOffsetMs - pushClockOffsetMs) >
    RESET_CLOCK_OFFSET_THRESHOLD_MS
  ) {
    lc.debug?.(
      'resetting client clockOffsetMs from',
      clockOffsetMs,
      'to',
      pushClockOffsetMs,
    );
    clockOffsetMs = pushClockOffsetMs;
  }

  const {clientGroupID} = body;
  const mutationClientIDs = new Set(body.mutations.map(m => m.clientID));
  const clientRecords = await getClientRecords(
    mutationClientIDs,
    IncludeDeleted.Include,
    storage,
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
        closeWithError(lc, client.socket, 'InvalidPush', errMsg, 'error');
        return;
      }
    } else {
      newClientIDs.push(mClientID);
    }
  }

  // tracks the highest index of a pending mutation
  let lastSamePusherAndClientGroupPendingMIndex = -1;
  const pendingDuplicates: Map<string, number> = new Map();
  for (let i = 0; i < pendingMutations.length; i++) {
    const pendingM = pendingMutations[i];
    if (
      pendingM.clientGroupID === clientGroupID &&
      pendingM.pusherClientIDs.has(clientID)
    ) {
      lastSamePusherAndClientGroupPendingMIndex = i;
    }
    previousMutationByClientID.set(pendingM.clientID, {
      id: pendingM.id,
      pendingIndex: i,
    });
    const range = mutationIdRangesByClientID.get(pendingM.clientID);
    if (range && range[0] <= pendingM.id && range[1] >= pendingM.id) {
      pendingDuplicates.set(pendingM.clientID + ':' + pendingM.id, i);
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
        lastSamePusherAndClientGroupPendingMIndex = Math.max(
          pendingDuplicateIndex,
          lastSamePusherAndClientGroupPendingMIndex,
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
        'InvalidPush',
        `Push contains unexpected mutation id ${m.id} for client ${
          m.clientID
        }. Expected mutation id ${previousMutationID + 1}.`,
      );
      return;
    }

    const normalizedTimestamp =
      m.clientID === clientID &&
      body.timestamp - m.timestamp < OLD_MUTATION_THRESHOLD_MS
        ? m.timestamp + clockOffsetMs
        : undefined;

    const mWithNormalizedTimestamp: PendingMutation = {
      name: m.name,
      id: m.id,
      clientID: m.clientID,
      args: m.args,
      clientGroupID,
      pusherClientIDs: new Set([clientID]),
      timestamps: normalizedTimestamp
        ? {
            normalizedTimestamp,
            originTimestamp: m.timestamp,
            serverReceivedTimestamp: timestamp,
          }
        : undefined,
      auth: client.auth,
    };

    let insertIndex =
      Math.max(
        lastSamePusherAndClientGroupPendingMIndex,
        previousPendingIndex,
      ) + 1;
    for (; insertIndex < pendingMutations.length; insertIndex++) {
      if (mWithNormalizedTimestamp.timestamps === undefined) {
        break;
      }
      const pendingM = pendingMutations[insertIndex];
      if (
        pendingM.timestamps !== undefined &&
        pendingM.timestamps.normalizedTimestamp >
          mWithNormalizedTimestamp.timestamps.normalizedTimestamp
      ) {
        break;
      }
    }

    // -1 because we are not modifying pendingMutation, and we still need
    // to check the next push mutation against the pendingMutation we
    // just selected to insert the current push mutation in front of.
    lastSamePusherAndClientGroupPendingMIndex = insertIndex - 1;
    previousMutationByClientID.set(m.clientID, {
      id: m.id,
      pendingIndex: insertIndex - 1,
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
          lastSeen: now(),
          userID: client.auth.userID,
        },
        storage,
      ),
    ),
  );

  // All validation and writes which can fail have been completed,
  // only now do we mutate client and pendingMutations.
  client.clockOffsetMs = clockOffsetMs;

  for (const i of pendingDuplicates.values()) {
    const pendingM = pendingMutations[i];
    const pusherClientIDs = new Set(pendingM.pusherClientIDs);
    pusherClientIDs.add(clientID);
    pendingMutations[i] = {
      ...pendingM,
      pusherClientIDs,
    };
  }

  if (inserts.length === 1) {
    pendingMutations.splice(inserts[0][0], 0, inserts[0][1]);
  } else if (inserts.length > 1) {
    if (pendingMutations.length === 0) {
      pendingMutations.push(...inserts.map(([, pendingM]) => pendingM));
    } else {
      // This copy approach is taken to ensure O(m + p).  Using splice
      // in place would result in O(m * p).
      const newPendingMutations = [];
      // merge into newPendingMutations
      let insertsIndex = 0;
      for (let i = 0; i < pendingMutations.length; ) {
        if (insertsIndex < inserts.length && inserts[insertsIndex][0] === i) {
          newPendingMutations.push(inserts[insertsIndex][1]);
          insertsIndex++;
        } else {
          newPendingMutations.push(pendingMutations[i]);
          i++;
        }
      }
      for (; insertsIndex < inserts.length; insertsIndex++) {
        newPendingMutations.push(inserts[insertsIndex][1]);
      }
      pendingMutations.splice(
        0,
        pendingMutations.length,
        ...newPendingMutations,
      );
    }
  }

  lc.debug?.(
    'inserted',
    inserts.length,
    'mutations, now there are',
    pendingMutations.length,
    'pending mutations.',
  );
  processUntilDone();
}
