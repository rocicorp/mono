import type {ClientID, ClientMap, ClientState} from '../types/client-state.js';
import type {PushBody} from '../protocol/push.js';
import type {LogContext} from '@rocicorp/logger';
import {
  ClientRecord,
  getClientRecord,
  putClientRecord,
} from '../types/client-record.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import type {PendingMutationMap} from '../types/mutation.js';
import {sendError} from '../util/socket.js';
import {must} from '../util/must.js';

export type Now = () => number;
export type ProcessUntilDone = () => void;

// Called when no pushes will ever succeed over this connection since the client
// is out of sync with the server. Close connection so client can try to
// reconnect and recover.
function sendErrorAndClose(
  lc: LogContext,
  client: ClientState,
  errorMsg: string,
) {
  lc.error?.('Sending error and closing socket.  Error:', errorMsg);
  const {socket} = client;
  sendError(socket, errorMsg);
  socket.close();
}

/**
 * handles the 'push' upstream message by queueing the mutations included in
 * [[body]] into pendingMutations.
 */
export async function handlePush(
  lc: LogContext,
  storage: DurableStorage,
  client: ClientState,
  clients: ClientMap,
  pendingMutations: PendingMutationMap,
  body: PushBody,
  now: Now,
  processUntilDone: ProcessUntilDone,
) {
  lc.addContext('push');
  lc.info?.('handling push', JSON.stringify(body));

  // TODO(greg): not sure of best handling of clockBehindByMs in
  // a client group model (push contains mutations from multiple clients)
  if (client.clockBehindByMs === undefined) {
    client.clockBehindByMs = now() - body.timestamp;
    lc.debug?.(
      'initializing clock offset: clock behind by',
      client.clockBehindByMs,
    );
  }
  const {clientGroupID} = body;
  const pending = [...(pendingMutations.get(clientGroupID) ?? [])];
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

  const expectedMutationIDByClientID = new Map();
  const newClientIDs: ClientID[] = [];
  for (const mClientID of mutationClientIDs) {
    const clientRecord = clientRecords.get(mClientID);
    expectedMutationIDByClientID.set(
      mClientID,
      (clientRecord?.lastMutationID ?? 0) + 1,
    );
    if (clientRecord) {
      if (clientRecord.clientGroupID !== clientGroupID) {
        sendErrorAndClose(
          lc,
          client,
          `Push with clientGroupID ${clientGroupID} contains mutation for client ${mClientID} which belongs to clientGroupID ${clientRecord.clientGroupID}.`,
        );
      }
    } else {
      newClientIDs.push(mClientID);
    }
  }
  for (const alreadyPending of pending) {
    expectedMutationIDByClientID.set(
      alreadyPending.clientID,
      alreadyPending.id + 1,
    );
  }

  for (const m of body.mutations) {
    const expectedMutationID = must(
      expectedMutationIDByClientID.get(m.clientID),
    );
    if (expectedMutationID > m.id) {
      lc.debug?.('mutation already applied', m.id);
      continue;
    }
    if (expectedMutationID < m.id) {
      sendErrorAndClose(
        lc,
        client,
        `Push contains unexpected mutation id ${m.id} for client ${m.clientID}. Expected mutation id ${expectedMutationID}.`,
      );
      return;
    }
    expectedMutationIDByClientID.set(m.clientID, m.id + 1);
    m.timestamp += clients.get(m.clientID)?.clockBehindByMs ?? 0;
    pending.push(m);
  }

  lc.debug?.(
    'inserted mutations, client group id',
    clientGroupID,
    'now has',
    pending.length,
    'pending mutations.',
  );
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
  pendingMutations.set(clientGroupID, pending);
  processUntilDone();
}
