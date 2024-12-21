import {expect, test} from 'vitest';
import {h64} from '../../shared/src/hash.js';
import type {AST} from './ast.js';
import {astSchema, normalizeAST} from './ast.js';
import {PROTOCOL_VERSION} from './protocol-version.js';

test('fields are placed into correct positions', () => {
  function normalizeAndStringify(ast: AST) {
    return JSON.stringify(normalizeAST(ast));
  }

  expect(
    normalizeAndStringify({
      alias: 'alias',
      table: 'table',
    }),
  ).toEqual(
    normalizeAndStringify({
      table: 'table',
      alias: 'alias',
    }),
  );

  expect(
    normalizeAndStringify({
      schema: 'schema',
      alias: 'alias',
      limit: 10,
      orderBy: [],
      related: [],
      where: undefined,
      table: 'table',
    }),
  ).toEqual(
    normalizeAndStringify({
      related: [],
      schema: 'schema',
      limit: 10,
      table: 'table',
      orderBy: [],
      where: undefined,
      alias: 'alias',
    }),
  );
});

test('conditions are sorted', () => {
  let ast: AST = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'b'},
          op: '=',
          right: {type: 'literal', value: 'value'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '=',
          right: {type: 'literal', value: 'value'},
        },
      ],
    },
  };

  expect(normalizeAST(ast).where).toEqual({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '=',
        right: {type: 'literal', value: 'value'},
      },
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: '=',
        right: {type: 'literal', value: 'value'},
      },
    ],
  });

  ast = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '=',
          right: {type: 'literal', value: 'y'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '=',
          right: {type: 'literal', value: 'x'},
        },
      ],
    },
  };

  expect(normalizeAST(ast).where).toEqual({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '=',
        right: {type: 'literal', value: 'x'},
      },
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '=',
        right: {type: 'literal', value: 'y'},
      },
    ],
  });

  ast = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '<',
          right: {type: 'literal', value: 'x'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '>',
          right: {type: 'literal', value: 'y'},
        },
      ],
    },
  };

  expect(normalizeAST(ast).where).toEqual({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '<',
        right: {type: 'literal', value: 'x'},
      },
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '>',
        right: {type: 'literal', value: 'y'},
      },
    ],
  });
});

test('related subqueries are sorted', () => {
  const ast: AST = {
    table: 'table',
    related: [
      {
        correlation: {parentField: ['a'], childField: ['a']},
        system: 'client',
        subquery: {
          table: 'table',
          alias: 'alias2',
        },
      },
      {
        correlation: {parentField: ['a'], childField: ['a']},
        system: 'client',
        subquery: {
          table: 'table',
          alias: 'alias1',
        },
      },
    ],
  };

  expect(normalizeAST(ast).related).toMatchInlineSnapshot(`
    [
      {
        "correlation": {
          "childField": [
            "a",
          ],
          "parentField": [
            "a",
          ],
        },
        "hidden": undefined,
        "subquery": {
          "alias": "alias1",
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "table",
          "where": undefined,
        },
        "system": "client",
      },
      {
        "correlation": {
          "childField": [
            "a",
          ],
          "parentField": [
            "a",
          ],
        },
        "hidden": undefined,
        "subquery": {
          "alias": "alias2",
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "table",
          "where": undefined,
        },
        "system": "client",
      },
    ]
  `);
});

test('protocol version', () => {
  const schemaJSON = JSON.stringify(astSchema);
  const hash = h64(schemaJSON).toString(36);

  // If this test fails because the AST schema has changed such that
  // old code will not understand the new schema, bump the
  // PROTOCOL_VERSION and update the expected values.
  expect(hash).toEqual('1g60qx4dfwety');
  expect(PROTOCOL_VERSION).toEqual(3);
});
