/**
 * Queries: registering and unregistering with the server, TTLs, batching,
 * transform errors, run() and preload(), and what views show.
 */
import type {Scenario} from '../harness.ts';
import {
  advance,
  connected,
  del,
  gotPut,
  issue,
  mutate,
  poke,
  put,
  queryHash,
  start,
  syncPoke,
  user,
  comment,
} from './helpers.ts';

const allHash = queryHash('issues.all');
const openHash = queryHash('issues.open');
const usersHash = queryHash('users.all');
const firstTwoHash = queryHash('issues.firstTwo');
const byOwnerU1 = queryHash('issues.byOwner', 'u1');
const byOwnerU2 = queryHash('issues.byOwner', 'u2');
const byIDi1 = queryHash('issues.byID', 'i1');

export const queryScenarios: Scenario[] = [
  {
    name: 'query: materialize registers the query with the default TTL; destroy unregisters it',
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'v', query: 'issues.all'},
      advance(10),
      {op: 'destroy', view: 'v'},
      advance(10),
    ],
  },
  {
    name: 'query: changes within queryChangeThrottleMs go in one message',
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'a', query: 'issues.all'},
      advance(5),
      {op: 'materialize', view: 'b', query: 'users.all'},
      advance(4),
      advance(1),
    ],
  },
  {
    name: 'query: a custom queryChangeThrottleMs',
    options: {queryChangeThrottleMs: 100},
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'v', query: 'issues.all'},
      advance(99),
      advance(1),
    ],
  },
  {
    name: 'query: materialize and destroy in one batch sends both',
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'v', query: 'issues.all'},
      {op: 'destroy', view: 'v'},
      advance(10),
    ],
  },
  {
    name: 'query: two views of one query share its registration',
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'a', query: 'issues.all'},
      {op: 'materialize', view: 'b', query: 'issues.all'},
      advance(10),
      ...syncPoke('p1', null, '01', [allHash], [put('issues', issue('i1'))]),
      {op: 'destroy', view: 'a'},
      advance(10),
      {op: 'destroy', view: 'b'},
      advance(10),
    ],
  },
  {
    name: 'query: a second view with a larger TTL re-registers the query; a smaller one does not',
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'a', query: 'issues.all', ttl: '1m'},
      advance(10),
      {op: 'materialize', view: 'b', query: 'issues.all', ttl: '2m'},
      advance(10),
      {op: 'materialize', view: 'c', query: 'issues.all', ttl: 30_000},
      advance(10),
    ],
  },
  {
    name: 'query: updateTTL to a larger TTL re-registers the query; a smaller one does not',
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'v', query: 'issues.all', ttl: '1m'},
      advance(10),
      {op: 'updateTTL', view: 'v', ttl: '3m'},
      advance(10),
      {op: 'updateTTL', view: 'v', ttl: '10s'},
      advance(10),
      {op: 'updateTTL', view: 'v', ttl: 'forever'},
      advance(10),
      {op: 'updateTTL', view: 'v', ttl: '1h'},
      advance(10),
    ],
  },
  {
    name: 'query: TTL forms, with anything above 10 minutes clamped',
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'a', query: 'issues.all', ttl: '1h'},
      {op: 'materialize', view: 'b', query: 'users.all', ttl: 'forever'},
      {
        op: 'materialize',
        view: 'c',
        query: 'issues.byOwner',
        args: 'u1',
        ttl: 'none',
      },
      {
        op: 'materialize',
        view: 'd',
        query: 'issues.byOwner',
        args: 'u2',
        ttl: 45_000,
      },
      {op: 'materialize', view: 'e', query: 'issues.open', ttl: '1.5m'},
      {op: 'materialize', view: 'f', query: 'issues.firstTwo', ttl: '30s'},
      advance(10),
    ],
  },
  {
    name: 'query: destroy while a mutation is pending waits for the mutation',
    steps: [
      start,
      connected,
      {op: 'materialize', view: 'v', query: 'issues.all'},
      advance(10),
      mutate('m1', 'issue.create', {id: 'i1'}),
      {op: 'destroy', view: 'v'},
      advance(10),
      ...poke('p1', null, '01', {lastMutationIDChanges: {$clientID: 1}}),
      advance(10),
    ],
  },
  {
    name: 'query: queries changed while disconnected are diffed against what the server has',
    steps: [
      {op: 'materialize', view: 'a', query: 'issues.all'},
      {op: 'materialize', view: 'b', query: 'users.all'},
      connected,
      ...poke('p1', null, '01', {
        desiredQueriesPatches: {
          $clientID: [
            {op: 'put', hash: allHash},
            {op: 'put', hash: usersHash},
          ],
        },
        gotQueriesPatch: [gotPut(allHash), gotPut(usersHash)],
      }),
      {op: 'socketClose', code: 1006},
      {op: 'socketClose', code: 1006},
      {op: 'destroy', view: 'a'},
      {op: 'materialize', view: 'c', query: 'issues.open'},
      advance(5000),
      connected,
      advance(10),
    ],
  },
  {
    name: 'query: queries with different args are different queries',
    steps: [
      {op: 'materialize', view: 'a', query: 'issues.byOwner', args: 'u1'},
      {op: 'materialize', view: 'b', query: 'issues.byOwner', args: 'u2'},
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [byOwnerU1, byOwnerU2],
        [
          put('issues', issue('i1', {ownerId: 'u1'})),
          put('issues', issue('i2', {ownerId: 'u2'})),
          put('issues', issue('i3', {ownerId: 'u2'})),
        ],
      ),
    ],
  },
  {
    name: 'query: one() is a row or nothing',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.byID', args: 'i1'},
      connected,
      ...syncPoke('p1', null, '01', [byIDi1]),
      ...syncPoke(
        'p2',
        '01',
        '02',
        [],
        [
          put('issues', issue('i1', {ownerId: 'u2'})),
          put('users', user('u2', 'Bob')),
        ],
      ),
      ...syncPoke('p3', '02', '03', [], [del('issues', {id: 'i1'})]),
    ],
  },
  {
    name: 'query: limit keeps the first rows in order and refills on delete',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.firstTwo'},
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [firstTwoHash],
        [
          put('issues', issue('i1', {title: 'C'})),
          put('issues', issue('i2', {title: 'A'})),
          put('issues', issue('i3', {title: 'B'})),
        ],
      ),
      ...syncPoke('p2', '01', '02', [], [del('issues', {id: 'i2'})]),
    ],
  },
  {
    name: 'query: related rows, and a row that stops matching',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.open'},
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [openHash],
        [put('issues', issue('i1')), put('issues', issue('i2'))],
      ),
      ...syncPoke('p2', '01', '02', [], [put('comments', comment('c1', 'i2'))]),
      ...syncPoke(
        'p3',
        '02',
        '03',
        [],
        [put('issues', issue('i2', {closed: true}))],
      ),
    ],
  },
  {
    name: 'query: an app transformError makes the view an error',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.byOwner', args: 'u2'},
      {op: 'materialize', view: 'w', query: 'issues.all'},
      connected,
      {
        op: 'receive',
        message: [
          'transformError',
          [
            {
              error: 'app',
              id: byOwnerU2,
              name: 'issues.byOwner',
              message: 'denied',
              details: {reason: 'not yours'},
            },
          ],
        ],
      },
      ...syncPoke('p1', null, '01', [byOwnerU2, allHash]),
    ],
  },
  {
    name: 'query: a parse transformError makes the view an error; unknown ids are ignored',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.all'},
      connected,
      {
        op: 'receive',
        message: [
          'transformError',
          [
            {
              error: 'parse',
              id: 'not-registered',
              name: 'issues.nope',
              message: 'bad args',
            },
            {
              error: 'parse',
              id: allHash,
              name: 'issues.all',
              message: 'bad args',
            },
          ],
        ],
      },
      {op: 'run', run: 'r', query: 'issues.all', type: 'complete'},
    ],
  },
  {
    name: 'run: unknown is what the store holds now',
    steps: [
      {op: 'run', run: 'r0', query: 'issues.all'},
      connected,
      ...poke('p1', null, '01', {rowsPatch: [put('issues', issue('i1'))]}),
      {op: 'run', run: 'r1', query: 'issues.all', type: 'unknown'},
      advance(10),
    ],
  },
  {
    name: 'run: complete waits for the server',
    steps: [
      start,
      connected,
      {op: 'run', run: 'r', query: 'issues.all', type: 'complete'},
      advance(10),
      ...syncPoke('p1', null, '01', [allHash], [put('issues', issue('i1'))]),
      advance(10),
    ],
  },
  {
    name: 'run and preload: cached accepts the previous connection, complete waits for this one',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.all'},
      connected,
      ...syncPoke('p1', null, '01', [allHash], [put('issues', issue('i1'))]),
      {op: 'socketClose', code: 1006},
      {op: 'run', run: 'cached', query: 'issues.all', type: 'cached'},
      {op: 'run', run: 'complete', query: 'issues.all', type: 'complete'},
      {op: 'preload', preload: 'p', query: 'issues.all'},
      {op: 'materialize', view: 'w', query: 'issues.all'},
      {op: 'materialize', view: 'u', query: 'users.all'},
      connected,
      ...poke('p2', '01', '02', {}),
    ],
  },
  {
    name: 'preload: registers with no TTL, resolves when the server has it, and cleans up',
    steps: [
      start,
      connected,
      {op: 'preload', preload: 'p', query: 'issues.all'},
      advance(10),
      ...syncPoke('p1', null, '01', [allHash], [put('issues', issue('i1'))]),
      {op: 'cleanup', preload: 'p'},
      advance(10),
      {op: 'preload', preload: 'q', query: 'users.all', ttl: '1m'},
      advance(10),
    ],
  },
  {
    name: 'preload: a transformError rejects both promises',
    steps: [
      start,
      connected,
      {op: 'preload', preload: 'p', query: 'issues.all'},
      {
        op: 'receive',
        message: [
          'transformError',
          [{error: 'app', id: allHash, name: 'issues.all'}],
        ],
      },
    ],
  },
];
