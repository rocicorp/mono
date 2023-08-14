import type {LogContext} from '@rocicorp/logger';
import type {Patch, Poke, PokeMessage} from 'reflect-protocol';
import type {BufferSizer} from 'shared/src/buffer-sizer.js';
import {must} from 'shared/src/must.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import type {ClientPoke} from '../types/client-poke.js';
import type {ClientID, ClientMap} from '../types/client-state.js';
import {getConnectedClients} from '../types/connected-clients.js';
import type {PendingMutation} from '../types/mutation.js';
import {randomID} from '../util/rand.js';
import {send} from '../util/socket.js';
import type {MutatorMap} from './process-mutation.js';
import {processRoom} from './process-room.js';

/**
 * Processes pending mutations and client disconnect/connects, and sends
 * relevant pokes.
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
  lc = lc.withContext('numClients', clients.size);
  lc.debug?.('process pending');

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
    lc.debug?.('No pending mutations or disconnects to process, exiting');
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

  lc = lc.withContext('numMutations', toProcess.length);
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
  // Performance optimization: when sending pokes to more than one client,
  // only JSON.stringify each unique patch once.  Other than fast-forward
  // patches, patches sent to clients are identical.  If they are large,
  // running JSON.stringify on them is slow and can be the dominate cost
  // of processPending.
  if (clients.size === 1) {
    const [clientID, client] = [...clients.entries()][0];
    const pokes = clientPokes
      .filter(clientPoke => clientPoke.clientID === clientID)
      .map(clientPoke => clientPoke.poke);
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
    return;
  }
  const pokesByClientID = new Map<ClientID, [Poke, string][]>();
  const patchStrings = new Map<Patch, string>();
  for (const clientPoke of clientPokes) {
    let pokes = pokesByClientID.get(clientPoke.clientID);
    if (!pokes) {
      pokes = [];
      pokesByClientID.set(clientPoke.clientID, pokes);
    }
    const {patch} = clientPoke.poke;
    let patchString = patchStrings.get(patch);
    if (patchString === undefined) {
      patchString = JSON.stringify(patch);
      patchStrings.set(clientPoke.poke.patch, patchString);
    }
    pokes.push([clientPoke.poke, patchString]);
  }
  // This manual json string building is necessary, to avoid JSON.stringify-ing
  // the same patches for each client.
  for (const [clientID, pokes] of pokesByClientID) {
    const client = must(clients.get(clientID));
    const pokeStrings = [];
    for (const [poke, patchString] of pokes) {
      const {patch: _, ...pokeMinusPatch} = poke;
      const pokeMinusPatchString = JSON.stringify(pokeMinusPatch);
      pokeStrings.push(
        pokeMinusPatchString.substring(0, pokeMinusPatchString.length - 1) +
          ',"patch":' +
          patchString +
          '}',
      );
    }
    const pokesString = `[${pokeStrings.join(',')}]`;
    const pokeMessageString =
      `["poke",{` +
      `"requestID":"${randomID()}",` +
      `${client.debugPerf ? `"debugServerBufferMs":${bufferMs},` : ''}` +
      `"pokes": ${pokesString}` +
      `}]`;
    lc.debug?.('sending client', clientID, 'poke');
    client.socket.send(pokeMessageString);
  }
}
