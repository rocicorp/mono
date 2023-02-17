// Processes all pending mutations from [[clients]] that are ready to be
// processed in one or more frames, up to [[endTime]] and sends necessary

import type {ClientMap} from '../types/client-state.js';
import type {PokeMessage} from '../protocol/poke.js';
import type {ClientPokeBody} from '../types/client-poke-body.js';
import type {LogContext} from '@rocicorp/logger';
import {must} from '../util/must.js';
import type {MutatorMap} from './process-mutation.js';
import {processRoom} from './process-room.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {send} from '../util/socket.js';
import type {PendingMutationMap} from '../types/mutation.js';

/**
 * Processes all mutations in all rooms for a time range, and send relevant pokes.
 * @param clients Rooms to process mutations for
 * @param mutators All known mutators
 */
export async function processPending(
  lc: LogContext,
  storage: DurableStorage,
  clients: ClientMap,
  pendingMutations: PendingMutationMap,
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  timestamp: number,
): Promise<void> {
  lc.debug?.('process pending');

  const t0 = Date.now();
  try {
    const pokes = await processRoom(
      lc,
      clients,
      pendingMutations,
      mutators,
      disconnectHandler,
      storage,
      timestamp,
    );
    sendPokes(lc, pokes, clients);
    lc.debug?.('clearing pending mutations');
    pendingMutations.clear();
  } finally {
    lc.debug?.(`processPending took ${Date.now() - t0} ms`);
  }
}

function sendPokes(
  lc: LogContext,
  pokes: ClientPokeBody[],
  clients: ClientMap,
) {
  for (const pokeBody of pokes) {
    const client = must(clients.get(pokeBody.clientID));
    const poke: PokeMessage = ['poke', pokeBody.poke];
    lc.debug?.('sending client', pokeBody.clientID, 'poke', pokeBody.poke);
    send(client.socket, poke);
  }
}
