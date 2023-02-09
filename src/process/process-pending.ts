// Processes all pending mutations from [[clients]] that are ready to be
// processed in one or more frames, up to [[endTime]] and sends necessary

import type {ClientID, ClientMap} from '../types/client-state.js';
import type {PokeBody, PokeMessage} from '../protocol/poke.js';
import type {ClientPokeBody} from '../types/client-poke-body.js';
import type {LogContext} from '@rocicorp/logger';
import {must} from '../util/must.js';
import type {MutatorMap} from './process-mutation.js';
import {processRoom} from './process-room.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import {send} from '../util/socket.js';
import type {TurnBuffer} from '../server/turn-buffer.js';

/**
 * Processes all mutations in all rooms for a time range, and send relevant pokes.
 * @param clients Rooms to process mutations for
 * @param mutators All known mutators
 */
export async function processPending(
  lc: LogContext,
  storage: DurableStorage,
  clients: ClientMap,
  mutators: MutatorMap,
  turnBuffer: TurnBuffer,
  disconnectHandler: DisconnectHandler,
  timestamp: number,
): Promise<void> {
  lc.debug?.('process pending');

  const t0 = Date.now();
  try {
    const pokes = await processRoom(
      lc,
      clients,
      mutators,
      turnBuffer,
      disconnectHandler,
      storage,
      timestamp,
    );
    sendPokes(lc, pokes, clients);
    clearPendingMutations(lc, pokes, clients);
  } finally {
    lc.debug?.(`processPending took ${Date.now() - t0} ms`);
  }
}

function sendPokes(
  lc: LogContext,
  pokes: ClientPokeBody[],
  clients: ClientMap,
) {
  const pokesByClientID = new Map<ClientID, PokeBody[]>();
  for (const pokeBody of pokes) {
    let arr = pokesByClientID.get(pokeBody.clientID);
    if (!arr) {
      arr = [];
      pokesByClientID.set(pokeBody.clientID, arr);
    }
    arr.push(pokeBody.poke);
  }
  for (const [clientID, pokeArr] of pokesByClientID) {
    const client = must(clients.get(clientID));
    const poke: PokeMessage = [
      'poke',
      pokeArr.length === 1 ? pokeArr[0] : pokeArr,
    ];
    lc.debug?.('sending client', clientID, 'poke', poke);
    send(client.socket, poke);
  }
}

function clearPendingMutations(
  lc: LogContext,
  pokes: ClientPokeBody[],
  clients: ClientMap,
) {
  lc.debug?.('clearing pending mutations');
  for (const pokeBody of pokes) {
    const client = must(clients.get(pokeBody.clientID));
    const idx = client.pending.findIndex(
      mutation => mutation.id > pokeBody.poke.lastMutationID,
    );
    client.pending.splice(0, idx > -1 ? idx : client.pending.length);
  }
}
