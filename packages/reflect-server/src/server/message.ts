import type {LogContext} from '@rocicorp/logger';
import {Upstream, upstreamSchema} from 'reflect-protocol';
import * as valita from 'shared/src/valita.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import type {ClientID, ClientMap} from '../types/client-state.js';
import type {PendingMutation} from '../types/mutation.js';
import {closeWithError, sendError, Socket} from '../util/socket.js';
import {handlePing} from './ping.js';
import {handlePull} from './pull.js';
import {handlePush, type ProcessUntilDone} from './push.js';

/**
 * Handles an upstream message coming into the server by dispatching to the
 * appropriate handler.
 */
export async function handleMessage(
  lc: LogContext,
  storage: DurableStorage,
  clients: ClientMap,
  pendingMutations: PendingMutation[],
  clientID: ClientID,
  data: string,
  ws: Socket,
  processUntilDone: ProcessUntilDone,
) {
  let message;
  try {
    message = getMessage(data);
  } catch (e) {
    sendError(lc, ws, 'InvalidMessage', String(e));
    return;
  }

  const client = clients.get(clientID);
  if (!client) {
    // This is not expected to ever occur.  However if it does no pushes will
    // ever succeed over this connection since it is missing an entry in
    // ClientMap.  Close connection so client can try to reconnect and recover.
    closeWithError(lc, ws, 'ClientNotFound', clientID);
    return;
  }

  lc = lc.withContext('msgType', message[0]);
  switch (message[0]) {
    case 'ping':
      handlePing(lc, ws);
      break;
    case 'push':
      await handlePush(
        lc,
        storage,
        clientID,
        clients,
        pendingMutations,
        message[1],
        () => Date.now(),
        processUntilDone,
      );
      break;
    case 'pull':
      await handlePull(storage, message[1], ws);
      break;
    default:
      throw new Error(`Unknown message type: ${message[0]}`);
  }
}

function getMessage(data: string): Upstream {
  const value = JSON.parse(data);
  return valita.parse(value, upstreamSchema);
}
