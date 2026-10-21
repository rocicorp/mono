/**
 * Mutations: optimistic writes, pushes, push responses, and rebasing pending
 * mutations on what the server sends.
 */
import type {Scenario} from '../harness.ts';
import {
  advance,
  connected,
  del,
  issue,
  mutate,
  poke,
  pushResponse,
  put,
  queryHash,
  start,
  syncPoke,
} from './helpers.ts';

const allHash = queryHash('issues.all');

const viewAll = {op: 'materialize', view: 'v', query: 'issues.all'} as const;

/** A deprecated push error body in a pushResponse. */
function pushError(name: string, body: Record<string, unknown>): Scenario {
  return {
    name: `mutate: a ${name} pushResponse error`,
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      {op: 'receive', message: ['pushResponse', body as never]},
      mutate('m2', 'issue.create', {id: 'i2'}),
    ],
  };
}

export const mutationScenarios: Scenario[] = [
  {
    name: 'mutate: an optimistic write shows at once and is pushed once',
    steps: [
      viewAll,
      connected,
      mutate('m1', 'issue.create', {id: 'i1', title: 'T', ownerId: 'u1'}),
      advance(10),
      mutate('m2', 'comment.add', {id: 'c1', issueId: 'i1'}),
      advance(10),
    ],
  },
  {
    name: 'mutate: mutations made while connecting are pushed once connected',
    steps: [
      viewAll,
      mutate('m1', 'issue.create', {id: 'i1'}),
      mutate('m2', 'issue.create', {id: 'i2'}),
      connected,
      advance(10),
    ],
  },
  {
    name: 'mutate: a reconnect pushes pending mutations again and sends lmid',
    steps: [
      viewAll,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      mutate('m2', 'issue.create', {id: 'i2'}),
      ...poke('p1', null, '01', {
        lastMutationIDChanges: {$clientID: 1},
        gotQueriesPatch: [{op: 'put', hash: allHash}],
        rowsPatch: [put('issues', issue('i1'))],
      }),
      {op: 'socketClose', code: 1006},
      connected,
      advance(10),
    ],
  },
  {
    name: 'mutate: an ok pushResponse settles the server promise with its data',
    steps: [
      viewAll,
      connected,
      mutate('m1', 'issue.createReturning', {id: 'i1'}),
      mutate('m2', 'issue.create', {id: 'i2'}),
      pushResponse([
        {id: 1, result: {data: {created: 'i1', at: 'server'}}},
        {id: 2, result: {}},
      ]),
      ...syncPoke(
        'p1',
        null,
        '01',
        [allHash],
        [put('issues', issue('i1')), put('issues', issue('i2'))],
      ),
      ...poke('p2', '01', '02', {lastMutationIDChanges: {$clientID: 2}}),
    ],
  },
  {
    name: 'mutate: an app error pushResponse rejects the server promise',
    steps: [
      viewAll,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      mutate('m2', 'issue.create', {id: 'i2'}),
      pushResponse([
        {
          id: 1,
          result: {error: 'app', message: 'denied', details: {code: 7}},
        },
        {id: 2, result: {error: 'app'}},
      ]),
      ...poke('p1', null, '01', {lastMutationIDChanges: {$clientID: 2}}),
    ],
  },
  {
    name: 'mutate: alreadyProcessed leaves the mutation for the lmid to settle',
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      pushResponse([{id: 1, result: {error: 'alreadyProcessed'}}]),
      ...poke('p1', null, '01', {lastMutationIDChanges: {$clientID: 1}}),
    ],
  },
  {
    name: 'mutate: oooMutation rejects the mutation and is an error',
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      mutate('m2', 'issue.create', {id: 'i2'}),
      pushResponse([
        {id: 1, result: {error: 'oooMutation', details: {expected: 0}}},
      ]),
      {op: 'connect'},
      connected,
      advance(10),
    ],
  },
  {
    name: 'mutate: a response for a settled mutation is ignored',
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      ...poke('p1', null, '01', {lastMutationIDChanges: {$clientID: 1}}),
      pushResponse([{id: 1, result: {error: 'app', message: 'late'}}]),
    ],
  },
  pushError('unsupportedPushVersion', {error: 'unsupportedPushVersion'}),
  pushError('unsupportedSchemaVersion', {error: 'unsupportedSchemaVersion'}),
  pushError('http 401', {
    error: 'http',
    status: 401,
    details: 'Unauthorized',
  }),
  pushError('http 500', {
    error: 'http',
    status: 500,
    details: 'Internal Server Error',
    mutationIDs: [{clientID: '$clientID', id: 1}],
  }),
  pushError('zeroPusher', {error: 'zeroPusher', details: 'fetch failed'}),
  {
    name: 'mutate: a mutator that throws is rejected on the client and not pushed',
    steps: [
      viewAll,
      connected,
      mutate('m1', 'issue.fail', 'boom'),
      mutate('m2', 'issue.createThenFail', {id: 'i1'}),
      advance(10),
      mutate('m3', 'issue.create', {id: 'i2'}),
    ],
  },
  {
    name: 'mutate: insert, upsert, update, and delete',
    steps: [
      viewAll,
      connected,
      mutate('m1', 'issue.create', {id: 'i1', title: 'one'}),
      mutate('m2', 'issue.create', {id: 'i1', title: 'again'}),
      mutate('m3', 'issue.upsert', {id: 'i1', title: 'upserted'}),
      mutate('m4', 'issue.upsert', {id: 'i2', title: 'new'}),
      mutate('m5', 'issue.update', {id: 'i2', closed: true}),
      mutate('m6', 'issue.update', {id: 'i9', title: 'missing'}),
      mutate('m7', 'issue.delete', 'i1'),
      mutate('m8', 'issue.delete', 'i9'),
    ],
  },
  {
    name: 'mutate: pending mutations are rebased on new server data',
    steps: [
      viewAll,
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [allHash],
        [put('issues', issue('i1')), put('issues', issue('i2'))],
      ),
      mutate('m1', 'issue.closeAllOpen'),
      ...syncPoke('p2', '01', '02', [], [put('issues', issue('i3'))]),
      ...poke('p3', '02', '03', {
        lastMutationIDChanges: {$clientID: 1},
        rowsPatch: [
          put('issues', issue('i1', {closed: true})),
          put('issues', issue('i2', {closed: true})),
        ],
      }),
    ],
  },
  {
    name: 'mutate: a rebase where the mutator now throws drops its writes',
    steps: [
      viewAll,
      connected,
      ...syncPoke('p1', null, '01', [allHash]),
      mutate('m1', 'issue.createUnique', {id: 'i1', title: 'mine'}),
      ...syncPoke(
        'p2',
        '01',
        '02',
        [],
        [put('issues', issue('i1', {title: 'theirs'}))],
      ),
      ...poke('p3', '02', '03', {lastMutationIDChanges: {$clientID: 1}}),
    ],
  },
  {
    name: 'mutate: a rebase where the row is gone',
    steps: [
      viewAll,
      connected,
      ...syncPoke('p1', null, '01', [allHash], [put('issues', issue('i1'))]),
      mutate('m1', 'issue.update', {id: 'i1', title: 'edited'}),
      ...syncPoke('p2', '01', '02', [], [del('issues', {id: 'i1'})]),
      ...poke('p3', '02', '03', {lastMutationIDChanges: {$clientID: 1}}),
    ],
  },
  {
    name: 'mutate: needs-auth keeps mutations and pushes them after connect()',
    options: {auth: 'token-1'},
    steps: [
      start,
      connected,
      mutate('m1', 'issue.create', {id: 'i1'}),
      {
        op: 'receive',
        message: [
          'error',
          {kind: 'AuthInvalidated', message: 'expired', origin: 'zeroCache'},
        ],
      },
      mutate('m2', 'issue.create', {id: 'i2'}),
      {op: 'connect', auth: 'token-2'},
      connected,
      advance(10),
    ],
  },
];
