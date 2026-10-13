import {expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {serverToClient} from '../../../zero-schema/src/name-mapper.ts';
import {STORAGE_VERSION} from './storage-version.ts';
import {mergePokes} from './zero-poke-handler.ts';

test('When what a client stores changes we may need to bump the storage version', () => {
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
  });

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
              desiredQueriesPatches: {
                c1: [
                  {op: 'put', hash: astHash},
                  {op: 'put', hash: nameAndArgsHash},
                ],
              },
              gotQueriesPatch: [{op: 'put', hash: astHash}],
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
                  op: 'put',
                  tableName: 'issue_labels',
                  value: {['issue_id']: 'i1', ['label_id']: 'l1'},
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

  // If this test fails, what a client stores in its local database has
  // changed. Check that this client can read what an older client stored and
  // that an older client can read what this one stores. If either can't, bump
  // STORAGE_VERSION, then update the expected values.
  expect(pullResponse.patch).toMatchInlineSnapshot(`
    [
      {
        "key": "d/c1/10w64mp0we24v9",
        "op": "put",
        "value": null,
      },
      {
        "key": "d/c1/0zm02yb1j5jkcj",
        "op": "put",
        "value": null,
      },
      {
        "key": "g/10w64mp0we24v9",
        "op": "put",
        "value": null,
      },
      {
        "key": "e/issue/i1",
        "op": "put",
        "value": {
          "id": "i1",
          "title": "a",
        },
      },
      {
        "constrain": undefined,
        "key": "e/issue/i2",
        "merge": {
          "title": "b",
        },
        "op": "update",
      },
      {
        "key": "e/issueLabel/139976146940794883839417541154829961763",
        "op": "put",
        "value": {
          "issueID": "i1",
          "labelID": "l1",
        },
      },
      {
        "key": "m/c1/1",
        "op": "put",
        "value": {
          "data": {
            "ok": true,
          },
        },
      },
      {
        "key": "m/c1/2",
        "op": "put",
        "value": {
          "details": {
            "code": 7,
          },
          "error": "app",
          "message": "nope",
        },
      },
    ]
  `);
  expect(STORAGE_VERSION).toBe(53);
});
