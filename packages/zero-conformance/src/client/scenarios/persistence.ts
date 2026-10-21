/**
 * Persistence: what a client relaunched on the same storage starts from, and
 * the lifecycle of the clients that share it.
 *
 * The JS client persists about a second after a change (Replicache's persist
 * scheduler), and the Swift client sooner, so every scenario lets the clock
 * run before it restarts: what each client loses in a crash right after a
 * change is not part of the contract.
 */
import type {Scenario, Step} from '../harness.ts';
import {
  advance,
  connected,
  issue,
  mutate,
  poke,
  put,
  queryHash,
  start,
  syncPoke,
} from './helpers.ts';

const allHash = queryHash('issues.all');

/** Lets the JS client persist what it has. */
const persisted = advance(2000);

const synced: Step[] = [
  {op: 'materialize', view: 'v', query: 'issues.all'},
  start,
  connected,
  ...syncPoke(
    'p1',
    null,
    '01',
    [allHash],
    [put('issues', issue('i1')), put('issues', issue('i2'))],
  ),
  persisted,
];

/** Syncs, leaves a mutation pending, then gets `kind` from zero-cache. */
function serverErrorThenRestart(name: string, kind: string): Scenario {
  return {
    name,
    steps: [
      ...synced,
      mutate('m1', 'issue.create', {id: 'i3'}),
      persisted,
      {
        op: 'receive',
        message: ['error', {kind, message: 'gone', origin: 'zeroCache'}],
      },
      persisted,
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      connected,
    ],
  };
}

export const persistenceScenarios: Scenario[] = [
  {
    name: 'persistence: rows, the cookie and got queries survive a restart',
    steps: [
      ...synced,
      {op: 'restart'},
      // The rows are there before the client connects; the query is cached,
      // not complete.
      {op: 'materialize', view: 'v', query: 'issues.all'},
      {op: 'run', run: 'cached', query: 'issues.all', type: 'cached'},
      {op: 'run', run: 'complete', query: 'issues.all', type: 'complete'},
      {op: 'preload', preload: 'p', query: 'issues.all'},
      start,
      connected,
      ...syncPoke('p2', '01', '02', [allHash]),
    ],
  },
  {
    name: 'persistence: a restart before the client connects keeps what it had',
    steps: [
      ...synced,
      {op: 'restart'},
      persisted,
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
    ],
  },
  {
    name: 'persistence: a pending mutation is pushed again by the next client',
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1', title: 'mine'}),
      persisted,
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      connected,
      ...poke('p1', null, '01', {
        lastMutationIDChanges: {'$clientID.0': 1},
        rowsPatch: [put('issues', issue('i1', {title: 'mine'}))],
      }),
      persisted,
      // Confirmed, so not pushed again.
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      connected,
    ],
  },
  {
    name: 'persistence: pending mutations of two clients are pushed in order',
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      persisted,
      {op: 'restart'},
      start,
      connected,
      mutate('m2', 'issue.close', 'i1'),
      persisted,
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      connected,
      ...poke('p1', null, '01', {
        lastMutationIDChanges: {'$clientID.0': 1},
        rowsPatch: [put('issues', issue('i1'))],
      }),
    ],
  },
  {
    name: 'persistence: a pending mutation is replayed on what the next client loads',
    steps: [
      ...synced,
      mutate('m1', 'issue.closeAllOpen'),
      persisted,
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      connected,
      // i3 arrives before the server runs the mutation, so the replay closes
      // it too.
      ...poke('p2', '01', '02', {
        rowsPatch: [put('issues', issue('i3'))],
      }),
    ],
  },
  {
    name: 'persistence: another set of mutators starts a new client group',
    steps: [
      ...synced,
      {op: 'restart', mutators: 'issues'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
    ],
  },
  {
    name: 'persistence: the same set of mutators joins the existing client group',
    steps: [
      ...synced,
      {op: 'restart', mutators: 'issues'},
      start,
      persisted,
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
    ],
  },
  {
    name: 'persistence: another user, storage key, or logged-out client starts empty',
    steps: [
      ...synced,
      {op: 'restart', options: {userID: 'u2'}},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      {op: 'restart', options: {storageKey: 'other'}},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      {op: 'restart', options: {userID: null}},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      // Back to the first user's storage.
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
    ],
  },
  {
    name: 'persistence: another mutate or query URL starts empty',
    steps: [
      ...synced,
      {op: 'restart', options: {mutateURL: 'http://api.test/mutate'}},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
      {op: 'restart', options: {queryURL: 'http://api.test/query'}},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
    ],
  },
  {
    name: 'persistence: a client inactive for a day is deleted and the server told',
    // No pings for the hour the scenario stays connected.
    options: {pingTimeoutMs: 3_600_000},
    steps: [
      start,
      connected,
      persisted,
      {op: 'restart', advance: 86_400_000},
      start,
      connected,
      // Client GC runs every five minutes.
      advance(300_000),
      // Not yet confirmed: sent again with the next connection.
      {op: 'socketClose'},
      advance(5000),
      connected,
      {op: 'receive', message: ['deleteClients', {clientIDs: ['$clientID.0']}]},
      {op: 'socketClose'},
      advance(5000),
      connected,
    ],
  },
  {
    name: 'persistence: a client active within the day is kept',
    options: {pingTimeoutMs: 3_600_000},
    steps: [
      start,
      connected,
      persisted,
      {op: 'restart', advance: 86_000_000},
      start,
      connected,
      advance(300_000),
    ],
  },
  // The server lost the client group: the next client starts a new one, and
  // the old group's pending mutations are not pushed (no mutation recovery).
  serverErrorThenRestart(
    'persistence: after ClientNotFound the next client starts a new client group',
    'ClientNotFound',
  ),
  serverErrorThenRestart(
    'persistence: after SchemaVersionNotSupported the next client starts a new client group',
    'SchemaVersionNotSupported',
  ),
  // The server is behind the client: the store is dropped.
  serverErrorThenRestart(
    'persistence: after InvalidConnectionRequestBaseCookie the next client starts from an empty store',
    'InvalidConnectionRequestBaseCookie',
  ),
  serverErrorThenRestart(
    'persistence: after InvalidConnectionRequestLastMutationID the next client starts from an empty store',
    'InvalidConnectionRequestLastMutationID',
  ),
  {
    name: 'persistence: after a message too large for the server the next client starts a new client group',
    // Nothing is sent before the close: the error lists recent messages, and
    // the Swift client writes object keys in another order than JS.
    steps: [
      start,
      connected,
      ...poke('p1', null, '01', {rowsPatch: [put('issues', issue('i1'))]}),
      persisted,
      {op: 'socketClose', code: 1009, reason: 'too big'},
      persisted,
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
    ],
  },
  {
    name: 'persistence: errors that keep the client group',
    steps: [
      ...synced,
      {
        op: 'receive',
        message: [
          'error',
          {kind: 'Unauthorized', message: 'expired', origin: 'zeroCache'},
        ],
      },
      persisted,
      {op: 'restart'},
      {op: 'materialize', view: 'v', query: 'issues.all'},
      start,
    ],
  },
];
