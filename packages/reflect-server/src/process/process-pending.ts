import type {ClientID, ClientMap} from '../types/client-state.js';
import type {Poke, PokeMessage} from 'reflect-protocol';
import type {ClientPoke} from '../types/client-poke.js';
import type {LogContext} from '@rocicorp/logger';
import {must} from '../util/must.js';
import type {MutatorMap} from './process-mutation.js';
import {processRoom} from './process-room.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {send} from '../util/socket.js';
import type {PendingMutationMap} from '../types/mutation.js';
import {randomID} from '../util/rand.js';

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
