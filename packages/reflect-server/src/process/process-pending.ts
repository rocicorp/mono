import type {LogContext} from '@rocicorp/logger';
import type {Patch, Poke} from 'reflect-protocol';
import type {Env} from 'reflect-shared/src/types.js';
import type {BufferSizer} from 'shared/src/buffer-sizer.js';
import {must} from 'shared/src/must.js';
import type {ClientDeleteHandler} from '../server/client-delete-handler.js';
import type {DisconnectHandler} from '../server/disconnect.js';
import type {DurableStorage} from '../storage/durable-storage.js';
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
  env: Env,
  storage: DurableStorage,
  clients: ClientMap,
  pendingMutations: PendingMutation[],
  mutators: MutatorMap,
  disconnectHandler: DisconnectHandler,
  clientDeleteHandler: ClientDeleteHandler,
  maxProcessedMutationTimestamp: number,
  bufferSizer: BufferSizer,
  maxMutationsToProcess: number,
  shouldGCClients: (now: number) => boolean,
): Promise<{maxProcessedMutationTimestamp: number; nothingToProcess: boolean}> {
  const start = Date.now();
  lc = lc.withContext('numClients', clients.size);
  lc.debug?.('process pending');
  const storedConnectedClients = await getConnectedClients(storage);
  lc.debug?.(`Got ${storedConnectedClients.size} connected clients`);
  let hasConnectsOrDisconnectsToProcess =
    storedConnectedClients.size !== clients.size;
  if (!hasConnectsOrDisconnectsToProcess) {
    for (const clientState of clients.values()) {
      if (!clientState.sentInitialPresence) {
        hasConnectsOrDisconnectsToProcess = true;
        break;
      }
    }
  }
  if (!hasConnectsOrDisconnectsToProcess) {
    for (const clientID of storedConnectedClients) {
      if (!clients.has(clientID)) {
        hasConnectsOrDisconnectsToProcess = true;
        break;
      }
    }
  }
  if (pendingMutations.length === 0 && !hasConnectsOrDisconnectsToProcess) {
    lc.debug?.(
      'No pending mutations, connects or disconnects to process, exiting',
    );
    return {maxProcessedMutationTimestamp, nothingToProcess: true};
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
  let missCount = 0;
  let bufferNeededMs = Number.MIN_SAFE_INTEGER;
  for (let i = 0; i < pendingMutations.length; i++) {
    const pendingM = pendingMutations[i];
    if (
      maxProcessedMutationTimestamp !== undefined &&
      pendingM.timestamps !== undefined &&
      pendingM.timestamps.normalizedTimestamp < maxProcessedMutationTimestamp
    ) {
      missCount++;
    }
    if (pendingM.timestamps !== undefined) {
      bufferNeededMs = Math.max(
        bufferNeededMs,
        pendingM.timestamps.serverReceivedTimestamp -
          pendingM.timestamps.normalizedTimestamp,
      );
    }
  }

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
  const pokesByClientID = await processRoom(
    lc,
    env,
    clients,
    pendingMutations,
    endIndex,
    mutators,
    disconnectHandler,
    clientDeleteHandler,
    storage,
    shouldGCClients,
  );
  sendPokes(lc, pokesByClientID, clients, bufferMs, start);
  lc.debug?.('clearing pending mutations');
  pendingMutations.splice(0, endIndex);
  for (const clientState of clients.values()) {
    clientState.sentInitialPresence = true;
  }
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
  pokesByClientID: Map<ClientID, Poke[]>,
  clients: ClientMap,
  bufferMs: number,
  start: number,
) {
  // Performance optimization: when sending pokes only JSON.stringify each
  // unique patch once.  Other than fast-forward patches, patches sent to
  // clients are identical.  If they are large, running JSON.stringify on them
  // for each client is slow and can be the dominate cost of processPending.
  const memoizedPatchStrings = new Map<Patch, MemoizedPatchString>();
  const memoizedPresencePatchStrings = new Map<Patch, MemoizedPatchString>();
  const patchesLogStrings = lc.info ? ['Patches:'] : undefined;
  const presenceLogStrings = lc.info ? ['Presence:'] : undefined;
  for (const pokes of pokesByClientID.values()) {
    for (const {patch, presence = []} of pokes) {
      memoize(patch, memoizedPatchStrings, patchesLogStrings);
      memoize(presence, memoizedPresencePatchStrings, presenceLogStrings);
    }
  }
  // This manual json string building is necessary, to avoid JSON.stringify-ing
  // the same patches for each client.
  let pokesForClientsLogString = 'Pokes:';
  for (const [clientID, pokes] of pokesByClientID) {
    const client = must(clients.get(clientID));
    let pokesString = '[';
    let pokesLogString = '[';
    for (let i = 0; i < pokes.length; i++) {
      const poke = pokes[i];
      const {patch, presence = [], ...pokeMinusPatchStrings} = poke;
      const pokeMinusPatchStringsString = JSON.stringify(pokeMinusPatchStrings);
      const pokeMinusPatchStringStringPrefix =
        pokeMinusPatchStringsString.substring(
          0,
          pokeMinusPatchStringsString.length - 1,
        );
      const memoizedPatchString = must(memoizedPatchStrings.get(patch));
      const memoizedPresenceString = must(
        memoizedPresencePatchStrings.get(presence),
      );
      pokesString += appendPatch(
        pokeMinusPatchStringStringPrefix,
        memoizedPatchString.string,
        memoizedPresenceString.string,
        i,
        pokes.length,
      );
      if (lc.info) {
        pokesLogString += appendPatch(
          pokeMinusPatchStringsString,
          memoizedPatchString.id,
          memoizedPresenceString.string,
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
  const processPendingLatencyMs = Date.now() - start;
  lc = lc
    .withContext('processPendingTiming', processPendingLatencyMs)
    .withContext('pokeCount', pokesByClientID.size);
  lc.info?.(
    'Finished process pending in ' +
      (Date.now() - start) +
      'ms. Sent ' +
      pokesByClientID.size +
      ' pokes.' +
      (pokesByClientID.size === 0
        ? ''
        : ' ' +
          pokesForClientsLogString +
          '\n' +
          patchesLogStrings?.join('') +
          '\n' +
          presenceLogStrings?.join('')),
  );
}

function memoize(
  patch: Patch,
  memoizedPatchStrings: Map<Patch, MemoizedPatchString>,
  logStrings: string[] | undefined,
) {
  let memoizedPatchString = memoizedPatchStrings.get(patch);
  if (memoizedPatchString === undefined) {
    memoizedPatchString = {
      string: JSON.stringify(patch),
      id: randomID(),
    };
    memoizedPatchStrings.set(patch, memoizedPatchString);
    logStrings?.push(
      ` ${memoizedPatchString.id}=${truncate(
        memoizedPatchString.string,
        MAX_PATCH_CHARS_TO_LOG,
      )}`,
    );
  }
}

function appendPatch(
  pokeMinusPatchStringPrefix: string,
  patchString: string,
  presenceString: string,
  i: number,
  length: number,
) {
  return (
    pokeMinusPatchStringPrefix +
    ',"patch":' +
    patchString +
    ',"presence":' +
    presenceString +
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
