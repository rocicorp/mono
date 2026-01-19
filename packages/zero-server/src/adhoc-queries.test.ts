import {describe, expect, test} from 'vitest';
import type {AST} from '../../zero-protocol/src/ast.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import {schema} from '../../zql/src/query/test/test-schemas.ts';
import {
  ADHOC_QUERY_NAME,
  executeAdhocQuery,
  isAdhocQueryName,
} from './adhoc-queries.ts';

describe('executeAdhocQuery', () => {
  test('creates a query from valid AST args', () => {
    const ast: AST = {
      table: 'issue',
      where: {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'id'},
        right: {type: 'literal', value: '123'},
      },
    };

    const query = executeAdhocQuery({ast}, {schema});

    const internals = asQueryInternals(query);
    expect(internals.ast.table).toBe('issue');
    expect(internals.ast.where).toEqual(ast.where);
  });

  test('creates a query with ordering and limit', () => {
    const ast: AST = {
      table: 'issue',
      orderBy: [['createdAt', 'desc']],
      limit: 10,
    };

    const query = executeAdhocQuery({ast}, {schema});

    const internals = asQueryInternals(query);
    expect(internals.ast.orderBy).toEqual([['createdAt', 'desc']]);
    expect(internals.ast.limit).toBe(10);
  });

  test('creates a query with related subqueries', () => {
    const ast: AST = {
      table: 'issue',
      related: [
        {
          correlation: {
            parentField: ['ownerId'],
            childField: ['id'],
          },
          subquery: {
            table: 'user',
            alias: 'owner',
          },
        },
      ],
    };

    const query = executeAdhocQuery({ast}, {schema});

    const internals = asQueryInternals(query);
    expect(internals.ast.related).toHaveLength(1);
    expect(internals.ast.related?.[0].subquery.table).toBe('user');
  });

  test('throws on invalid args - missing ast property', () => {
    expect(() => executeAdhocQuery({invalid: 'args'}, {schema})).toThrow(
      'Missing property ast',
    );
  });

  test('throws on undefined args', () => {
    expect(() => executeAdhocQuery(undefined, {schema})).toThrow(
      'Expected object. Got undefined',
    );
  });

  test('throws on null args', () => {
    expect(() => executeAdhocQuery(null, {schema})).toThrow(
      'Expected object. Got null',
    );
  });

  test('throws on missing ast field', () => {
    expect(() => executeAdhocQuery({}, {schema})).toThrow(
      'Missing property ast',
    );
  });
});

describe('isAdhocQueryName', () => {
  test('returns true for ADHOC_QUERY_NAME', () => {
    expect(isAdhocQueryName(ADHOC_QUERY_NAME)).toBe(true);
    expect(isAdhocQueryName('_zero_adhoc')).toBe(true);
  });

  test('returns false for other names', () => {
    expect(isAdhocQueryName('myQuery')).toBe(false);
    expect(isAdhocQueryName('_zero_crud')).toBe(false);
    expect(isAdhocQueryName('')).toBe(false);
  });
});

describe('ADHOC_QUERY_NAME', () => {
  test('is a reserved name starting with _zero_', () => {
    expect(ADHOC_QUERY_NAME).toBe('_zero_adhoc');
    expect(ADHOC_QUERY_NAME.startsWith('_zero_')).toBe(true);
  });
});
