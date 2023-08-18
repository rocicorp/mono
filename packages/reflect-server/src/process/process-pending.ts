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
  maxMutationsToProcess: number,
): Promise<{maxProcessedMutationTimestamp: number; nothingToProcess: boolean}> {
  const start = Date.now();
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

  const bufferMs = bufferSizer.bufferSizeMs;
  let endIndex = pendingMutations.length;
  for (let i = 0; i < pendingMutations.length; i++) {
    if (i >= maxMutationsToProcess) {
      lc.info?.(
        'turn size limited by maxMutationsPerTurn',
        maxMutationsToProcess,
      );
      endIndex = i;
      break;
    }
    const pendingM = pendingMutations[i];
    if (
      pendingM.timestamps !== undefined &&
      pendingM.timestamps.normalizedTimestamp > start - bufferMs
    ) {
      endIndex = i;
      break;
    }
  }
  const toProcessMutations = pendingMutations.slice(0, endIndex);
  const missCount =
    maxProcessedMutationTimestamp === undefined
      ? 0
      : toProcessMutations.reduce(
          (sum, pendingM) =>
            sum +
            (pendingM.timestamps !== undefined &&
            pendingM.timestamps.normalizedTimestamp <
              maxProcessedMutationTimestamp
              ? 1
              : 0),
          0,
        );

  const bufferNeededMs = toProcessMutations.reduce(
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
    bufferSizer.recordMissable(start, missCount > 0, bufferNeededMs, lc);
  }

  lc = lc
    .withContext('numPending', pendingMutations.length)
    .withContext('numMutations', toProcessMutations.length);
  lc.info?.(
    'Starting process pending',
    {
      toProcessMutations: toProcessMutations.length,
      pendingMutations: pendingMutations.length,
      hasConnectsOrDisconnectsToProcess,
      missCount,
    },
    toProcessMutations,
  );
  const pokes = await processRoom(
    lc,
    clients,
    toProcessMutations,
    mutators,
    disconnectHandler,
    storage,
  );
  sendPokes(lc, pokes, clients, bufferMs, start);
  lc.debug?.('clearing pending mutations');
  pendingMutations.splice(0, endIndex);
  return {
    nothingToProcess: false,
    maxProcessedMutationTimestamp: toProcessMutations.reduce<number>(
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
  start: number,
) {
  // Performance optimization: when sending pokes only JSON.stringify each
  // unique patch once.  Other than fast-forward patches, patches sent to
  // clients are identical.  If they are large, running JSON.stringify on them
  // for each client is slow and can be the dominate cost of processPending.
  const pokesByClientID = new Map<ClientID, [Poke, MemoizedPatchString][]>();
  const memoizedPatchStrings = new Map<Patch, MemoizedPatchString>();
  let patchesLogString = 'Patches:';
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
      if (lc.info) {
        patchesLogString += ` ${memoizedPatchString.id}=${truncate(
          memoizedPatchString.string,
          MAX_PATCH_CHARS_TO_LOG,
        )}`;
      }
    }
    pokes.push([clientPoke.poke, memoizedPatchString]);
  }
  // This manual json string building is necessary, to avoid JSON.stringify-ing
  // the same patches for each client.
  let pokesForClientsLogString = 'Pokes:';
  for (const [clientID, pokes] of pokesByClientID) {
    const client = must(clients.get(clientID));
    let pokesString = '[';
    let pokesLogString = '[';
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
      if (lc.info) {
        pokesLogString += appendPatch(
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
    if (lc.info) {
      pokesLogString += ']';
      const pokeMessageLogString = makePokeMessage(
        requestID,
        client,
        bufferMs,
        pokesLogString,
      );
      pokesForClientsLogString += ` ${clientID}=${pokeMessageLogString}`;
    }
  }
  lc.info?.(
    'Finished process pending in ' +
      (Date.now() - start) +
      'ms. Sent ' +
      pokesByClientID.size +
      ' pokes.' +
      (pokesByClientID.size === 0
        ? ''
        : ' ' + pokesForClientsLogString + '\n' + patchesLogString),
  );
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
  return str.substring(0, maxLength) + `...(${maxLength}/${str.length} chars)`;
}
