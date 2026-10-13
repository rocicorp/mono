import {expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {
  CRUD_MUTATION_NAME,
  type CRUDMutationArg,
} from '../../../zero-protocol/src/mutation.ts';
import {pokePartBodySchema} from '../../../zero-protocol/src/poke.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {serverToClient} from '../../../zero-schema/src/name-mapper.ts';
import {defineMutators} from '../../../zql/src/mutate/mutator-registry.ts';
import {defineMutator} from '../../../zql/src/mutate/mutator.ts';
import {makeCRUDMutateBatch} from './crud.ts';
import {makeReplicacheMutators} from './make-replicache-mutators.ts';
import {STORAGE_VERSION} from './storage-version.ts';
import {mergePokes} from './zero-poke-handler.ts';

// If a test in this file fails, what a client stores in its local database has
// changed. Check that this client can read what an older client stored and
// that an older client can read what this one stores. If either can't, bump
// STORAGE_VERSION, then update the expected values. They are written out
// rather than snapshotted so that `vitest -u` can't update them.

const schema = createSchema({
  tables: [
    table('issue')
      .from('issues')
      .columns({
        id: string().from('issue_id'),
        title: string(),
      })
      .primaryKey('id'),
    table('issueLabel')
      .from('issue_labels')
      .columns({
        issueID: string().from('issue_id'),
        labelID: string().from('label_id'),
      })
      .primaryKey('issueID', 'labelID'),
  ],
  enableLegacyMutators: true,
});

test('When what a client stores changes we may need to bump the storage version', () => {
  // A poke part field that isn't in the poke below may be something new for
  // the client to store. Add it to the poke.
  expect(Object.keys(pokePartBodySchema.shape).sort()).toEqual([
    'desiredQueriesPatches',
    'gotQueriesPatch',
    'lastMutationIDChanges',
    'mutationsPatch',
    'pokeID',
    'rowsPatch',
  ]);

  // Stored queries are keyed by these hashes.
  const ast: AST = {
    table: 'issues',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'title'},
          right: {type: 'literal', value: 'a'},
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {parentField: ['issue_id'], childField: ['issue_id']},
            subquery: {table: 'issue_labels', alias: 'zsubq_labels'},
            system: 'client',
          },
        },
      ],
    },
    related: [
      {
        correlation: {parentField: ['issue_id'], childField: ['issue_id']},
        subquery: {table: 'issue_labels', alias: 'labels'},
        system: 'client',
      },
    ],
    orderBy: [['issue_id', 'asc']],
    limit: 10,
  };
  const astHash = hashOfAST(ast);
  const nameAndArgsHash = hashOfNameAndArgs('issuesByLabel', [
    {label: 'bug', limit: 10},
  ]);

  const {pullResponse} = must(
    mergePokes(
      [
        {
          pokeStart: {pokeID: 'p1', baseCookie: '1'},
          parts: [
            {
              pokeID: 'p1',
              lastMutationIDChanges: {c1: 2},
              desiredQueriesPatches: {
                c1: [
                  {op: 'put', hash: astHash, ttl: 60_000},
                  {op: 'put', hash: nameAndArgsHash},
                  {op: 'del', hash: 'oldhash'},
                ],
              },
              gotQueriesPatch: [
                {op: 'put', hash: astHash},
                {op: 'del', hash: 'oldhash'},
              ],
              rowsPatch: [
                {
                  op: 'put',
                  tableName: 'issues',
                  value: {['issue_id']: 'i1', title: 'a'},
                },
                {
                  op: 'update',
                  tableName: 'issues',
                  id: {['issue_id']: 'i2'},
                  merge: {title: 'b'},
                },
                {
                  op: 'del',
                  tableName: 'issues',
                  id: {['issue_id']: 'i3'},
                },
                {
                  op: 'put',
                  tableName: 'issue_labels',
                  value: {['issue_id']: 'i1', ['label_id']: 'l1'},
                },
                {
                  op: 'del',
                  tableName: 'issue_labels',
                  id: {['issue_id']: 'i1', ['label_id']: 'l1'},
                },
              ],
              mutationsPatch: [
                {
                  op: 'put',
                  mutation: {
                    id: {clientID: 'c1', id: 1},
                    result: {data: {ok: true}},
                  },
                },
                {
                  op: 'put',
                  mutation: {
                    id: {clientID: 'c1', id: 2},
                    result: {error: 'app', message: 'nope', details: {code: 7}},
                  },
                },
                {op: 'del', id: {clientID: 'c1', id: 1}},
              ],
            },
          ],
          pokeEnd: {pokeID: 'p1', cookie: '2'},
        },
      ],
      schema,
      serverToClient(schema.tables),
    ),
  );
  assert('patch' in pullResponse, 'Expected a pull response with a patch');

  expect(pullResponse).toEqual({
    cookie: '2',
    lastMutationIDChanges: {c1: 2},
    patch: [
      {op: 'put', key: 'd/c1/10w64mp0we24v9', value: null},
      {op: 'put', key: 'd/c1/0zm02yb1j5jkcj', value: null},
      {op: 'del', key: 'd/c1/oldhash'},
      {op: 'put', key: 'g/10w64mp0we24v9', value: null},
      {op: 'del', key: 'g/oldhash'},
      {op: 'put', key: 'e/issue/i1', value: {id: 'i1', title: 'a'}},
      {op: 'update', key: 'e/issue/i2', merge: {title: 'b'}},
      {op: 'del', key: 'e/issue/i3'},
      {
        op: 'put',
        key: 'e/issueLabel/139976146940794883839417541154829961763',
        value: {issueID: 'i1', labelID: 'l1'},
      },
      {
        op: 'del',
        key: 'e/issueLabel/139976146940794883839417541154829961763',
      },
      {op: 'put', key: 'm/c1/1', value: {data: {ok: true}}},
      {
        op: 'put',
        key: 'm/c1/2',
        value: {error: 'app', message: 'nope', details: {code: 7}},
      },
      {op: 'del', key: 'm/c1/1'},
    ],
  });
  expect(STORAGE_VERSION).toBe(52);
});

test('What a client stores for pending mutations', async () => {
  // A pending mutation is stored with its mutator's name and its args. The
  // next client to open the database pushes it.
  const lc = createSilentLogContext();
  expect(
    Object.keys(
      makeReplicacheMutators(
        schema,
        defineMutators({issue: {create: defineMutator(async () => {})}}),
        {},
        lc,
      ),
    ),
  ).toEqual([CRUD_MUTATION_NAME, 'issue.create']);
  expect(
    Object.keys(
      makeReplicacheMutators(schema, {issue: {close: async () => {}}}, {}, lc),
    ),
  ).toEqual([CRUD_MUTATION_NAME, 'issue|close']);
  expect(CRUD_MUTATION_NAME).toBe('_zero_crud');

  const stored: CRUDMutationArg[] = [];
  const mutateBatch = makeCRUDMutateBatch(schema, {
    [CRUD_MUTATION_NAME]: arg => {
      stored.push(arg);
      return Promise.resolve();
    },
  });
  await mutateBatch(async m => {
    await m.issue.insert({id: 'i1', title: 'a'});
    await m.issue.upsert({id: 'i2', title: 'b'});
    await m.issue.update({id: 'i1', title: 'c'});
    await m.issueLabel.delete({issueID: 'i1', labelID: 'l1'});
  });
  expect(stored).toEqual([
    {
      ops: [
        {
          op: 'insert',
          tableName: 'issue',
          primaryKey: ['id'],
          value: {id: 'i1', title: 'a'},
        },
        {
          op: 'upsert',
          tableName: 'issue',
          primaryKey: ['id'],
          value: {id: 'i2', title: 'b'},
        },
        {
          op: 'update',
          tableName: 'issue',
          primaryKey: ['id'],
          value: {id: 'i1', title: 'c'},
        },
        {
          op: 'delete',
          tableName: 'issueLabel',
          primaryKey: ['issueID', 'labelID'],
          value: {issueID: 'i1', labelID: 'l1'},
        },
      ],
    },
  ]);
});
