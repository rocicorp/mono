import type {ClientID, ClientMap} from '../types/client-state.js';
import type {Poke, PokeMessage} from 'reflect-protocol';
import type {ClientPoke} from '../types/client-poke.js';
import type {LogContext} from '@rocicorp/logger';
import {must} from 'shared/must.js';
import type {MutatorMap} from './process-mutation.js';
import {processRoom} from './process-room.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {send} from '../util/socket.js';
import type {PendingMutation} from '../types/mutation.js';
import {randomID} from '../util/rand.js';
import {getConnectedClients} from '../types/connected-clients.js';
import type {BufferSizer} from 'shared/buffer-sizer.js';

const CLIENT_INACTIVITY_THRESHOLD_MS = 10_000;

/**
 * Processes pending mutations, inactive clients and client disconnect/connects,
 * and sends relevant pokes.
 * @param clients Rooms to process mutations for
 * @param mutators All known mutators
 */
export async function processPending(
  lc: LogContext,
  storage: DurableStorage,
  clients: ClientMap,
  pendingMutations: PendingMutation[],
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  maxProcessedMutationTimestamp: number,
  bufferSizer: BufferSizer,
): Promise<{maxProcessedMutationTimestamp: number; nothingToProcess: boolean}> {
  lc.debug?.('process pending');
  const now = Date.now();
  for (const [clientID, client] of clients) {
    if (now - client.lastActivityTimestamp > CLIENT_INACTIVITY_THRESHOLD_MS) {
      lc.debug?.('closing socket', clientID, 'due to inactivity');
      client.socket.close();
      clients.delete(clientID);
    }
  }

  const storedConnectedClients = await getConnectedClients(storage);
  let hasConnectsOrDisconnectsToProcess = false;
  if (storedConnectedClients.size === clients.size) {
    for (const clientID of storedConnectedClients) {
      if (!clients.has(clientID)) {
        hasConnectsOrDisconnectsToProcess = true;
        break;
      }
    }
  } else {
    hasConnectsOrDisconnectsToProcess = true;
  }
  if (pendingMutations.length === 0 && !hasConnectsOrDisconnectsToProcess) {
    return {maxProcessedMutationTimestamp, nothingToProcess: true};
    lc.debug?.(
      'No pending mutations or connects/disconnects to process, exiting',
    );
  }

  const t0 = Date.now();
  const bufferMs = bufferSizer.bufferSizeMs;
  const tooNewIndex = pendingMutations.findIndex(
    pendingM =>
      pendingM.timestamps !== undefined &&
      pendingM.timestamps.normalizedTimestamp > t0 - bufferMs,
  );
  const endIndex = tooNewIndex !== -1 ? tooNewIndex : pendingMutations.length;
  const toProcess = pendingMutations.slice(0, endIndex);
  const missCount =
    maxProcessedMutationTimestamp === undefined
      ? 0
      : toProcess.reduce(
          (sum, pendingM) =>
            sum +
            (pendingM.timestamps !== undefined &&
            pendingM.timestamps.normalizedTimestamp <
              maxProcessedMutationTimestamp
              ? 1
              : 0),
          0,
        );

  const bufferNeededMs = toProcess.reduce(
    (max, pendingM) =>
      pendingM.timestamps === undefined
        ? max
        : Math.max(
            max,
            pendingM.timestamps.serverReceivedTimestamp -
              pendingM.timestamps.normalizedTimestamp,
          ),
    Number.MIN_SAFE_INTEGER,
  );

  if (bufferNeededMs !== Number.MIN_SAFE_INTEGER) {
    bufferSizer.recordMissable(t0, missCount > 0, bufferNeededMs, lc);
  }

  lc.debug?.(
    'processing',
    toProcess.length,
    'of',
    pendingMutations.length,
    'pending mutations with',
    missCount,
    'forced misses',
  );
  try {
    const pokes = await processRoom(
      lc,
      clients,
      toProcess,
      mutators,
      disconnectHandler,
      storage,
    );
    sendPokes(lc, pokes, clients, bufferMs);
    lc.debug?.('clearing pending mutations');
    pendingMutations.splice(0, endIndex);
  } finally {
    lc.debug?.(`processPending took ${Date.now() - t0} ms`);
  }
  return {
    nothingToProcess: false,
    maxProcessedMutationTimestamp: toProcess.reduce<number>(
      (max, processed) =>
        Math.max(max, processed.timestamps?.normalizedTimestamp ?? max),
      maxProcessedMutationTimestamp,
    ),
  };
}

function sendPokes(
  lc: LogContext,
  clientPokes: ClientPoke[],
  clients: ClientMap,
  bufferMs: number,
) {
  const pokesByClientID = new Map<ClientID, Poke[]>();
  for (const clientPoke of clientPokes) {
    let pokes = pokesByClientID.get(clientPoke.clientID);
    if (!pokes) {
      pokes = [];
      pokesByClientID.set(clientPoke.clientID, pokes);
    }
    pokes.push(clientPoke.poke);
  }
  for (const [clientID, pokes] of pokesByClientID) {
    const client = must(clients.get(clientID));
    const pokeMessage: PokeMessage = [
      'poke',
      {
        pokes,
        requestID: randomID(),
        debugServerBufferMs: client.debugPerf ? bufferMs : undefined,
      },
    ];
    lc.debug?.('sending client', clientID, 'poke', pokeMessage);
    send(client.socket, pokeMessage);
  }
}
