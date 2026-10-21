/**
 * Pokes: the pokeStart/pokePart/pokeEnd protocol, binary chunks, merging
 * pokes that arrive together, cookies, and what each patch does to the store.
 */
import type {ReadonlyJSONValue} from '../../../../shared/src/json.ts';
import type {Scenario, Step} from '../harness.ts';
import {
  comment,
  connected,
  del,
  gotDel,
  gotPut,
  issue,
  mutate,
  poke,
  pokeMessages,
  put,
  queryHash,
  syncPoke,
  user,
  advance,
} from './helpers.ts';

const allHash = queryHash('issues.all');
const openHash = queryHash('issues.open');
const usersHash = queryHash('users.all');
const labelsHash = queryHash('issueLabels.all');

const viewAll: Step = {op: 'materialize', view: 'v', query: 'issues.all'};

const receive = (message: ReadonlyJSONValue): Step => ({
  op: 'receive',
  message,
});
const pokeStart = (pokeID: string, baseCookie: string | null): Step =>
  receive(['pokeStart', {pokeID, baseCookie}]);
const pokeEnd = (pokeID: string, cookie: string): Step =>
  receive(['pokeEnd', {pokeID, cookie}]);

const issueLabel = (issueId: string, labelId: string) => ({issueId, labelId});

// A poke body with a multi-byte character, for splitting inside it.
const cafeParts = JSON.stringify([
  {
    pokeID: 'p1',
    gotQueriesPatch: [gotPut(allHash)],
    rowsPatch: [put('issues', issue('i1', {title: 'Café'}))],
  },
]);
const cafeAt = cafeParts.indexOf('é');

export const pokeScenarios: Scenario[] = [
  {
    name: 'poke: rows and a got query make a view complete',
    steps: [
      viewAll,
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [allHash],
        [put('issues', issue('i2')), put('issues', issue('i1'))],
      ),
      advance(10),
    ],
  },
  {
    name: 'poke: rows without a got query leave the view unknown',
    steps: [
      viewAll,
      connected,
      ...poke('p1', null, '01', {rowsPatch: [put('issues', issue('i1'))]}),
      ...syncPoke('p2', '01', '02', [allHash]),
    ],
  },
  {
    name: 'poke: server table and column names map to the client schema',
    steps: [
      {op: 'materialize', view: 'v', query: 'issues.open'},
      connected,
      receive(['pokeStart', {pokeID: 'p1', baseCookie: null, timestamp: 5}]),
      receive([
        'pokePart',
        {
          pokeID: 'p1',
          gotQueriesPatch: [gotPut(openHash)],
          rowsPatch: [
            put('issues', issue('i1', {ownerId: 'u2', createdAt: 2})),
            put('issues', issue('i2', {closed: true})),
            put('issues', issue('i3', {createdAt: 1})),
            put('comments', comment('c2', 'i1', {text: 'second'})),
            put('comments', comment('c1', 'i1', {text: 'first'})),
            put('comments', comment('c3', 'i2')),
            put(
              'users',
              user('u2', 'Bob', {registrar: 'github', email: 'b@x.test'}),
            ),
          ],
        },
      ]),
      pokeEnd('p1', '01'),
    ],
  },
  {
    name: 'poke: update merges server columns into the row',
    steps: [
      viewAll,
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [allHash],
        [put('issues', issue('i1', {ownerId: 'u2'}))],
      ),
      ...poke('p2', '01', '02', {
        rowsPatch: [
          {
            op: 'update',
            tableName: 'issues',
            id: {id: 'i1'},
            merge: {title: 'Renamed', owner_id: 'u3'},
          },
        ],
      }),
      ...poke('p3', '02', '03', {
        rowsPatch: [
          {
            op: 'update',
            tableName: 'issues',
            id: {id: 'i1'},
            merge: {closed: true},
            constrain: [],
          },
        ],
      }),
    ],
  },
  {
    name: 'poke: update with constrain keeps only the listed server columns',
    steps: [
      viewAll,
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [allHash],
        [put('issues', issue('i1', {ownerId: 'u2'}))],
      ),
      ...poke('p2', '01', '02', {
        rowsPatch: [
          {
            op: 'update',
            tableName: 'issues',
            id: {id: 'i1'},
            merge: {description: 'kept', owner_id: 'u9'},
            constrain: ['id', 'title', 'description', 'closed', 'createdAt'],
          },
        ],
      }),
      // The open view keeps the dropped column; a new one shows the store.
      {op: 'materialize', view: 'w', query: 'issues.all'},
    ],
  },
  {
    name: 'poke: del removes a row; deleting a missing row does nothing',
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
      ...poke('p2', '01', '02', {
        rowsPatch: [del('issues', {id: 'i1'}), del('issues', {id: 'i9'})],
      }),
    ],
  },
  {
    name: 'poke: clear removes every row',
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
      ...poke('p2', '01', '02', {rowsPatch: [{op: 'clear'}]}),
      ...syncPoke('p3', '02', '03', [allHash], [put('issues', issue('i3'))]),
      {op: 'materialize', view: 'w', query: 'issues.all'},
    ],
  },
  {
    name: 'poke: rows for a table the client does not know are skipped',
    steps: [
      viewAll,
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [allHash],
        [
          put('issueNotifications', {id: 'n1', issue_id: 'i1'}),
          put('issues', issue('i1')),
          del('issueNotifications', {id: 'n0'}),
        ],
      ),
    ],
  },
  {
    name: 'poke: rows with a compound primary key',
    steps: [
      {op: 'materialize', view: 'v', query: 'issueLabels.all'},
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [labelsHash],
        [
          put('issueLabel', issueLabel('i1', 'l2')),
          put('issueLabel', issueLabel('i2', 'l1')),
          put('issueLabel', issueLabel('i1', 'l1')),
        ],
      ),
      ...poke('p2', '01', '02', {
        rowsPatch: [del('issueLabel', {labelId: 'l1', issueId: 'i1'})],
      }),
    ],
  },
  {
    // A gotQueriesPatch clear is a replicache clear: it removes rows too.
    name: 'poke: gotQueriesPatch del and clear',
    steps: [
      viewAll,
      {op: 'materialize', view: 'u', query: 'users.all'},
      connected,
      ...syncPoke(
        'p1',
        null,
        '01',
        [allHash, usersHash],
        [put('issues', issue('i1'))],
      ),
      ...poke('p2', '01', '02', {gotQueriesPatch: [gotDel(allHash)]}),
      {op: 'materialize', view: 'v2', query: 'issues.all'},
      ...poke('p3', '02', '03', {gotQueriesPatch: [{op: 'clear'}]}),
      {op: 'materialize', view: 'u2', query: 'users.all'},
      ...syncPoke('p4', '03', '04', [allHash, usersHash]),
    ],
  },
  {
    name: 'poke: desiredQueriesPatches for this client decide what a reconnect sends',
    steps: [
      viewAll,
      {op: 'materialize', view: 'u', query: 'users.all'},
      connected,
      advance(10),
      ...poke('p1', null, '01', {
        desiredQueriesPatches: {
          $clientID: [
            {op: 'put', hash: allHash},
            {op: 'put', hash: 'stale-query'},
          ],
          otherClient: [{op: 'put', hash: usersHash}],
        },
        gotQueriesPatch: [gotPut(allHash)],
      }),
      {op: 'socketClose', code: 1006},
      connected,
      advance(10),
    ],
  },
  {
    name: 'poke: binary chunks carry the poke parts',
    steps: [
      viewAll,
      connected,
      pokeStart('p1', null),
      {
        op: 'receiveChunk',
        parts: [
          {pokeID: 'p1', gotQueriesPatch: [gotPut(allHash)]},
          {pokeID: 'p1', rowsPatch: [put('issues', issue('i1'))]},
        ],
      },
      pokeEnd('p1', '01'),
      pokeStart('p2', '01'),
      {op: 'receiveChunk', parts: []},
      pokeEnd('p2', '02'),
    ],
  },
  {
    name: 'poke: binary chunks split anywhere, even inside a code point',
    steps: [
      viewAll,
      connected,
      pokeStart('p1', null),
      {op: 'receiveChunk', text: cafeParts.slice(0, cafeAt)},
      {op: 'receiveChunk', bytes: [0xc3]},
      {op: 'receiveChunk', bytes: [0xa9]},
      {op: 'receiveChunk', text: cafeParts.slice(cafeAt + 1)},
      pokeEnd('p1', '01'),
    ],
  },
  {
    name: 'poke: a binary chunk without pokeStart is an error',
    steps: [
      viewAll,
      connected,
      {op: 'receiveChunk', parts: [{pokeID: 'p1'}]},
      {op: 'connect'},
      connected,
    ],
  },
  {
    name: 'poke: pokePart then a binary chunk in one poke is an error',
    steps: [
      viewAll,
      connected,
      pokeStart('p1', null),
      receive(['pokePart', {pokeID: 'p1', gotQueriesPatch: [gotPut(allHash)]}]),
      {op: 'receiveChunk', parts: [{pokeID: 'p1'}]},
      pokeEnd('p1', '01'),
    ],
  },
  {
    name: 'poke: a binary chunk then a pokePart in one poke is an error',
    steps: [
      viewAll,
      connected,
      pokeStart('p1', null),
      {op: 'receiveChunk', parts: [{pokeID: 'p1'}]},
      receive(['pokePart', {pokeID: 'p1', gotQueriesPatch: [gotPut(allHash)]}]),
    ],
  },
  {
    name: 'poke: binary chunks that are not poke parts are an error at pokeEnd',
    steps: [
      viewAll,
      connected,
      pokeStart('p1', null),
      {op: 'receiveChunk', parts: [{rowsPatch: []}]},
      pokeEnd('p1', '01'),
    ],
  },
  {
    name: 'poke: pokeStart while still receiving a poke is an error',
    steps: [
      viewAll,
      connected,
      pokeStart('p1', null),
      pokeStart('p2', null),
      advance(5000),
      connected,
    ],
  },
  {
    name: 'poke: a pokePart for another poke is an error',
    steps: [
      viewAll,
      connected,
      pokeStart('p1', null),
      receive(['pokePart', {pokeID: 'p2', rowsPatch: []}]),
    ],
  },
  {
    name: 'poke: a pokeEnd for another poke is an error',
    steps: [viewAll, connected, pokeStart('p1', null), pokeEnd('p2', '01')],
  },
  {
    name: 'poke: a pokePart with no poke in progress is an error',
    steps: [
      viewAll,
      connected,
      receive(['pokePart', {pokeID: 'p1', rowsPatch: []}]),
    ],
  },
  {
    name: 'poke: pokeEnd with cancel discards the poke',
    steps: [
      viewAll,
      connected,
      pokeStart('p1', null),
      receive([
        'pokePart',
        {
          pokeID: 'p1',
          gotQueriesPatch: [gotPut(allHash)],
          rowsPatch: [put('issues', issue('i1'))],
        },
      ]),
      receive(['pokeEnd', {pokeID: 'p1', cookie: '', cancel: true}]),
      ...syncPoke('p2', null, '01', [allHash], [put('issues', issue('i2'))]),
    ],
  },
  {
    name: 'poke: pokes that arrive together are applied as one',
    steps: [
      viewAll,
      connected,
      {
        op: 'receiveAll',
        messages: [
          ...pokeMessages('p1', null, '01', {
            gotQueriesPatch: [gotPut(allHash)],
            rowsPatch: [put('issues', issue('i1')), put('issues', issue('i3'))],
          }),
          ...pokeMessages('p2', '01', '02', {
            rowsPatch: [put('issues', issue('i2')), del('issues', {id: 'i1'})],
          }),
        ],
      },
    ],
  },
  {
    name: 'poke: pokes that arrive apart are applied one by one',
    steps: [
      viewAll,
      connected,
      {
        op: 'receiveAll',
        messages: pokeMessages('p1', null, '01', {
          gotQueriesPatch: [gotPut(allHash)],
          rowsPatch: [put('issues', issue('i1')), put('issues', issue('i3'))],
        }),
      },
      {
        op: 'receiveAll',
        messages: pokeMessages('p2', '01', '02', {
          rowsPatch: [put('issues', issue('i2')), del('issues', {id: 'i1'})],
        }),
      },
    ],
  },
  {
    name: 'poke: a cookie gap between pokes that arrive together is an error',
    steps: [
      viewAll,
      connected,
      {
        op: 'receiveAll',
        messages: [
          ...pokeMessages('p1', null, '01', {
            gotQueriesPatch: [gotPut(allHash)],
          }),
          ...pokeMessages('p2', '02', '03', {
            rowsPatch: [put('issues', issue('i1'))],
          }),
        ],
      },
      {op: 'connect'},
      connected,
    ],
  },
  {
    name: 'poke: a base cookie behind the previous cookie in pokes that arrive together is accepted',
    steps: [
      viewAll,
      connected,
      {
        op: 'receiveAll',
        messages: [
          ...pokeMessages('p1', null, '02', {
            gotQueriesPatch: [gotPut(allHash)],
          }),
          ...pokeMessages('p2', '01', '03', {
            rowsPatch: [put('issues', issue('i1'))],
          }),
        ],
      },
    ],
  },
  {
    name: 'poke: an unexpected base cookie reconnects from the stored cookie',
    steps: [
      viewAll,
      connected,
      ...syncPoke('p1', null, '01', [allHash], [put('issues', issue('i1'))]),
      ...syncPoke('p2', '05', '06', [], [put('issues', issue('i2'))]),
      advance(5000),
      connected,
      ...syncPoke('p3', '01', '02', [], [put('issues', issue('i2'))]),
    ],
  },
  {
    name: 'poke: a poke that does not advance the cookie is ignored',
    steps: [
      viewAll,
      connected,
      ...syncPoke('p1', null, '01', [allHash], [put('issues', issue('i1'))]),
      ...syncPoke('p2', '01', '01', [], [put('issues', issue('i2'))]),
    ],
  },
  {
    name: 'poke: a cookie that goes backwards is an error',
    steps: [
      viewAll,
      connected,
      ...syncPoke('p1', null, '02', [allHash], [put('issues', issue('i1'))]),
      ...syncPoke('p2', '02', '01', [], [put('issues', issue('i2'))]),
    ],
  },
  {
    name: 'poke: lastMutationIDChanges for this client settle and drop pending mutations',
    steps: [
      viewAll,
      connected,
      mutate('m1', 'issue.create', {id: 'i1', title: 'mine'}),
      mutate('m2', 'issue.create', {id: 'i2', title: 'mine too'}),
      ...poke('p1', null, '01', {
        lastMutationIDChanges: {$clientID: 1, otherClient: 7},
        gotQueriesPatch: [gotPut(allHash)],
        rowsPatch: [put('issues', issue('i1', {title: 'from server'}))],
      }),
      ...poke('p2', '01', '02', {lastMutationIDChanges: {$clientID: 2}}),
    ],
  },
  {
    name: 'poke: lastMutationIDChanges going backwards is an error',
    steps: [
      viewAll,
      connected,
      ...poke('p1', null, '01', {lastMutationIDChanges: {otherClient: 3}}),
      ...poke('p2', '01', '02', {lastMutationIDChanges: {otherClient: 2}}),
    ],
  },
  {
    name: 'poke: mutationsPatch results settle mutations and are acknowledged',
    steps: [
      viewAll,
      connected,
      mutate('m1', 'issue.createReturning', {id: 'i1'}),
      mutate('m2', 'issue.create', {id: 'i2'}),
      mutate('m3', 'issue.create', {id: 'i3'}),
      ...poke('p1', null, '01', {
        lastMutationIDChanges: {$clientID: 2},
        gotQueriesPatch: [gotPut(allHash)],
        rowsPatch: [put('issues', issue('i1'))],
        mutationsPatch: [
          {
            op: 'put',
            mutation: {
              id: {clientID: '$clientID', id: 1},
              result: {data: {created: 'i1', server: true}},
            },
          },
          {
            op: 'put',
            mutation: {
              id: {clientID: '$clientID', id: 2},
              result: {error: 'app', message: 'nope', details: {why: 'x'}},
            },
          },
        ],
      }),
      ...poke('p2', '01', '02', {
        mutationsPatch: [
          {op: 'del', id: {clientID: '$clientID', id: 1}},
          {op: 'del', id: {clientID: '$clientID', id: 2}},
        ],
      }),
      ...poke('p3', '02', '03', {
        lastMutationIDChanges: {$clientID: 3},
        rowsPatch: [put('issues', issue('i3'))],
      }),
    ],
  },
];
