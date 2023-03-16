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

// TODO: make buffer dynamic
const PENDING_ORDER_BUFFER_MS = 200;

/**
 * Processes all mutations in all rooms for a time range, and send relevant
 * pokes.
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
): Promise<number> {
  lc.debug?.('process pending');

  const t0 = Date.now();
  const tooNewIndex = pendingMutations.findIndex(
    pendingM =>
      pendingM.timestamp !== undefined &&
      pendingM.timestamp > t0 - PENDING_ORDER_BUFFER_MS,
  );
  const endIndex = tooNewIndex !== -1 ? tooNewIndex : pendingMutations.length;
  const toProcess = pendingMutations.slice(0, endIndex);
  const forcedMissCount =
    maxProcessedMutationTimestamp === undefined
      ? 0
      : toProcess.reduce(
          (sum, pendingM) =>
            sum +
            (pendingM.timestamp !== undefined &&
            pendingM.timestamp < maxProcessedMutationTimestamp
              ? 1
              : 0),
          0,
        );
  lc.debug?.(
    'processing',
    toProcess.length,
    'of',
    pendingMutations.length,
    'pending mutations with',
    forcedMissCount,
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
    sendPokes(lc, pokes, clients);
    lc.debug?.('clearing pending mutations');
    pendingMutations.splice(0, endIndex);
  } finally {
    lc.debug?.(`processPending took ${Date.now() - t0} ms`);
  }
  return toProcess.reduce<number>(
    (max, processed) => Math.max(max, processed.timestamp ?? max),
    maxProcessedMutationTimestamp,
  );
}

function sendPokes(
  lc: LogContext,
  clientPokes: ClientPoke[],
  clients: ClientMap,
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
      },
    ];
    lc.debug?.('sending client', clientID, 'poke', pokeMessage);
    send(client.socket, pokeMessage);
  }
}
