import {expect, test} from 'vitest';
import {AST, normalizeAST} from './ast.js';

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
      where: [],
      table: 'table',
    }),
  ).toEqual(
    normalizeAndStringify({
      related: [],
      schema: 'schema',
      limit: 10,
      table: 'table',
      orderBy: [],
      where: [],
      alias: 'alias',
    }),
  );
});

test('conditions are sorted', () => {
  let ast: AST = {
    table: 'table',
    where: [
      {
        type: 'simple',
        field: 'b',
        op: '=',
        value: 'value',
      },
      {
        type: 'simple',
        field: 'a',
        op: '=',
        value: 'value',
      },
    ],
  };

  expect(normalizeAST(ast).where).toEqual([
    {
      type: 'simple',
      field: 'a',
      op: '=',
      value: 'value',
    },
    {
      type: 'simple',
      field: 'b',
      op: '=',
      value: 'value',
    },
  ]);

  ast = {
    table: 'table',
    where: [
      {
        type: 'simple',
        field: 'a',
        op: '=',
        value: 'y',
      },
      {
        type: 'simple',
        field: 'a',
        op: '=',
        value: 'x',
      },
    ],
  };

  expect(normalizeAST(ast).where).toEqual([
    {
      type: 'simple',
      field: 'a',
      op: '=',
      value: 'x',
    },
    {
      type: 'simple',
      field: 'a',
      op: '=',
      value: 'y',
    },
  ]);

  ast = {
    table: 'table',
    where: [
      {
        type: 'simple',
        field: 'a',
        op: '<',
        value: 'x',
      },
      {
        type: 'simple',
        field: 'a',
        op: '>',
        value: 'y',
      },
    ],
  };

  expect(normalizeAST(ast).where).toEqual([
    {
      type: 'simple',
      field: 'a',
      op: '<',
      value: 'x',
    },
    {
      type: 'simple',
      field: 'a',
      op: '>',
      value: 'y',
    },
  ]);
});

test('related subqueries are sorted', () => {
  const ast: AST = {
    table: 'table',
    related: [
      {
        correlation: {
          parentField: 'a',
          childField: 'a',
          op: '=',
        },
        subquery: {
          table: 'table',
          alias: 'alias2',
        },
      },
      {
        correlation: {
          parentField: 'a',
          childField: 'a',
          op: '=',
        },
        subquery: {
          table: 'table',
          alias: 'alias1',
        },
      },
    ],
  };

  expect(normalizeAST(ast).related).toEqual([
    {
      correlation: {
        parentField: 'a',
        childField: 'a',
        op: '=',
      },
      subquery: {
        table: 'table',
        alias: 'alias1',
      },
    },
    {
      correlation: {
        parentField: 'a',
        childField: 'a',
        op: '=',
      },
      subquery: {
        table: 'table',
        alias: 'alias2',
        where: undefined,
        limit: undefined,
        orderBy: undefined,
        schema: undefined,
        related: undefined,
      },
    },
  ]);
});