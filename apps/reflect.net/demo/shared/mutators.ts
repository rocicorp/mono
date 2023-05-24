import type {ReadTransaction, WriteTransaction} from '@rocicorp/reflect';
import {
  deleteClient,
  ensureClient,
  initClient,
  putClient,
  updateClient,
} from '../alive/client-model';
import {alive, unload} from '../alive/orchestrator-model';
import {PIECE_DEFINITIONS} from '../alive/piece-definitions';
import {
  PieceModel,
  listPieces,
  putPiece,
  updatePiece,
} from '../alive/piece-model';

export type M = typeof mutators;

const clientConsoleMap = new Map<string, (log: string) => void>();

export function registerClientConsole(
  clientId: string,
  log: (log: string) => void,
) {
  clientConsoleMap.set(clientId, log);
}

export function deregisterClientConsole(clientId: string) {
  clientConsoleMap.delete(clientId);
}

const logsPrefix = 'refect-server-log';
export const entriesPrefix = `${logsPrefix}/entries/`;
export const entriesCountKey = `${logsPrefix}/count`;

function entriesKey(count: number): string {
  return `${entriesPrefix}${count.toString().padStart(10, '0')}`;
}

export async function getServerLogCount(tx: WriteTransaction): Promise<number> {
  return ((await tx.get(entriesCountKey)) as number) ?? 0;
}

export async function getServerLogs(tx: ReadTransaction): Promise<string[]> {
  return (await tx
    .scan({prefix: entriesPrefix})
    .values()
    .toArray()) as string[];
}

export async function addServerLog(tx: WriteTransaction, log: string) {
  const count = await getServerLogCount(tx);
  await tx.put(entriesKey(count), log);
  await tx.put(entriesCountKey, count + 1);
}

export const mutators = {
  resetRoom: async (tx: WriteTransaction) => {
    for (const key of await tx.scan().keys().toArray()) {
      await tx.del(key);
    }
  },
  solve: async (tx: WriteTransaction) => {
    for (const piece of await listPieces(tx)) {
      const def = PIECE_DEFINITIONS[parseInt(piece.id)];
      await updatePiece(tx, {
        ...piece,
        x: def.x,
        y: def.y,
        rotation: 0,
        placed: true,
      });
    }
  },

  // alive mutators
  initializePuzzle: async (
    tx: WriteTransaction,
    {force, pieces}: {force: boolean; pieces: PieceModel[]},
  ) => {
    if (!force && (await tx.get('puzzle-exists'))) {
      console.debug('puzzle already exists, skipping non-force initialization');
      return;
    }
    if (tx.environment === 'server') {
      //only reset on server and only when completed
      //removes potential race when multiple clients are trying to reset
      if (
        !force ||
        (await listPieces(tx)).findIndex(piece => !piece.placed) === -1
      ) {
        for (const piece of pieces) {
          await putPiece(tx, piece);
        }
        await tx.put('puzzle-exists', true);
      }
    }
  },

  putClient: wrapToFilterBadLocation(putClient),
  initClient: wrapToFilterBadLocation(initClient),
  updateClient: wrapToFilterBadLocation(updateClient),
  deleteClient,
  ensureClient: wrapToFilterBadLocation(ensureClient),
  updatePiece,

  // These mutators are for the how it works demos
  increment: async (
    tx: WriteTransaction,
    {key, delta}: {key: string; delta: number},
  ) => {
    const prev = ((await tx.get(key)) as number) ?? 0;
    const next = prev + delta;
    await tx.put(key, next);

    const prevStr = prev % 1 === 0 ? prev.toString() : prev.toFixed(2);
    const nextStr = next % 1 === 0 ? next.toString() : next.toFixed(2);
    const msg = `Running mutation ${tx.clientID}@${tx.mutationID} on ${tx.environment}: ${prevStr} → ${nextStr}`;

    if (tx.environment === 'client') {
      if (tx.reason !== 'rebase') {
        clientConsoleMap.get(tx.clientID)?.(msg);
      }
    } else {
      await addServerLog(tx, msg);
    }
  },
  degree: async (
    tx: WriteTransaction,
    {key, deg}: {key: string; deg: number},
  ) => {
    await tx.put(key, deg);
    const msg = `Running mutation ${tx.clientID}@${tx.mutationID} on ${tx.environment}: ${deg}`;

    if (tx.environment === 'client') {
      if (tx.reason !== 'rebase') {
        clientConsoleMap.get(tx.clientID)?.(msg);
      }
    } else {
      await addServerLog(tx, msg);
    }
  },
  addServerLog,
  getServerLogs,
  getServerLogCount,
  nop: async (_: WriteTransaction) => {},

  // orchestrator mutators
  alive,
  unload,
};

function filterBadLocationForClient<
  C extends {location?: string | null | undefined},
>(client: C): C {
  return client.location === undefined || allowLocation(client.location)
    ? client
    : {...client, location: null};
}

function allowLocation(location: string | null): boolean {
  // TODO(arv): Expand these as needed.
  return (
    typeof location === 'string' &&
    !/\.\/\\:<>\|/.test(location) &&
    // Note: this includes the flag and space too.
    location.length <= 24
  );
}

function wrapToFilterBadLocation<
  R,
  C extends {location?: string | null | undefined},
>(fn: (tx: WriteTransaction, client: C) => R) {
  return (tx: WriteTransaction, client: C) =>
    fn(tx, filterBadLocationForClient(client));
}
