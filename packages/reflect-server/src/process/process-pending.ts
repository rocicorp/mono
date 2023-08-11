import type {LogContext} from '@rocicorp/logger';
import type {Patch, Poke} from 'reflect-protocol';
import type {BufferSizer} from 'shared/src/buffer-sizer.js';
import {must} from 'shared/src/must.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {DurableStorage} from '../storage/durable-storage.js';
import type {ClientPoke} from '../types/client-poke.js';
import type {ClientID, ClientMap, ClientState} from '../types/client-state.js';
import {getConnectedClients} from '../types/connected-clients.js';
import type {PendingMutation} from '../types/mutation.js';
import {randomID} from '../util/rand.js';
import type {MutatorMap} from './process-mutation.js';
import {processRoom} from './process-room.js';
import {timed} from 'shared/src/timed.js';

/**
 * Processes pending mutations and client disconnect/connects, and sends
 * relevant pokes.
 * @param clients Rooms to process mutations for
 * @param mutators All known mutators
 */
export function processPending(
  lc: LogContext,
  storage: DurableStorage,
  clients: ClientMap,
  pendingMutations: PendingMutation[],
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  maxProcessedMutationTimestamp: number,
  bufferSizer: BufferSizer,
): Promise<{maxProcessedMutationTimestamp: number; nothingToProcess: boolean}> {
  lc = lc = lc.withContext('numClients', clients.size);
  return timed(lc.debug, 'processPending', () =>
    processPendingTimed(
      lc,
      storage,
      clients,
      pendingMutations,
      mutators,
      disconnectHandler,
      maxProcessedMutationTimestamp,
      bufferSizer,
    ),
  );
}

async function processPendingTimed(
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
  return {
    nothingToProcess: false,
    maxProcessedMutationTimestamp: toProcess.reduce<number>(
      (max, processed) =>
        Math.max(max, processed.timestamps?.normalizedTimestamp ?? max),
      maxProcessedMutationTimestamp,
    ),
  };
}

type MemoizedPatchString = {string: string; id: string};
const MAX_PATCH_CHARS_TO_LOG = 5000;

function sendPokes(
  lc: LogContext,
  clientPokes: ClientPoke[],
  clients: ClientMap,
  bufferMs: number,
) {
  // Performance optimization: when sending pokes only JSON.stringify each
  // unique patch once.  Other than fast-forward patches, patches sent to
  // clients are identical.  If they are large, running JSON.stringify on them
  // for each client is slow and can be the dominate cost of processPending.
  const pokesByClientID = new Map<ClientID, [Poke, MemoizedPatchString][]>();
  const memoizedPatchStrings = new Map<Patch, MemoizedPatchString>();
  for (const clientPoke of clientPokes) {
    let pokes = pokesByClientID.get(clientPoke.clientID);
    if (!pokes) {
      pokes = [];
      pokesByClientID.set(clientPoke.clientID, pokes);
    }
    const {patch} = clientPoke.poke;
    let memoizedPatchString = memoizedPatchStrings.get(patch);
    if (memoizedPatchString === undefined) {
      memoizedPatchString = {string: JSON.stringify(patch), id: randomID()};
      memoizedPatchStrings.set(clientPoke.poke.patch, memoizedPatchString);
      lc.debug?.(
        'stringifyed patch id',
        memoizedPatchString.id,
        'string',
        truncate(memoizedPatchString.string, MAX_PATCH_CHARS_TO_LOG),
      );
    }
    pokes.push([clientPoke.poke, memoizedPatchString]);
  }
  // This manual json string building is necessary, to avoid JSON.stringify-ing
  // the same patches for each client.
  for (const [clientID, pokes] of pokesByClientID) {
    const client = must(clients.get(clientID));
    let pokesString = '[';
    let debugPokesString = '[';
    for (let i = 0; i < pokes.length; i++) {
      const [poke, memoizedPatchString] = pokes[i];
      const {patch: _, ...pokeMinusPatch} = poke;
      const pokeMinusPatchString = JSON.stringify(pokeMinusPatch);
      const pokeMinusPatchStringPrefix = pokeMinusPatchString.substring(
        0,
        pokeMinusPatchString.length - 1,
      );
      pokesString += appendPatch(
        pokeMinusPatchStringPrefix,
        memoizedPatchString.string,
        i,
        pokes.length,
      );
      if (lc.debug) {
        debugPokesString += appendPatch(
          pokeMinusPatchStringPrefix,
          memoizedPatchString.id,
          i,
          pokes.length,
        );
      }
    }
    pokesString += ']';
    const requestID = randomID();
    const pokeMessageString = makePokeMessage(
      requestID,
      client,
      bufferMs,
      pokesString,
    );
    client.socket.send(pokeMessageString);

    if (lc.debug) {
      debugPokesString += ']';
      const debugPokeMessageString = makePokeMessage(
        requestID,
        client,
        bufferMs,
        debugPokesString,
      );
      lc.debug?.('sending client', clientID, 'poke', debugPokeMessageString);
    }
  }
}

function appendPatch(
  pokeMinusPatchStringPrefix: string,
  patchString: string,
  i: number,
  length: number,
) {
  return (
    pokeMinusPatchStringPrefix +
    ',"patch":' +
    patchString +
    '}' +
    (i === length - 1 ? '' : ',')
  );
}

function makePokeMessage(
  requestID: string,
  client: ClientState,
  bufferMs: number,
  pokesString: string,
) {
  return (
    `["poke",{` +
    `"requestID":"${requestID}",` +
    `${client.debugPerf ? `"debugServerBufferMs":${bufferMs},` : ''}` +
    `"pokes":${pokesString}` +
    `}]`
  );
}

function truncate(str: string, maxLength: number) {
  if (str.length < maxLength) {
    return str;
  }
  return str.substring(0, maxLength) + `(${maxLength}/${str.length} chars)`;
}
