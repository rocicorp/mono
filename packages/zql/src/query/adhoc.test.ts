import {describe, expect, test} from 'vitest';
import {ADHOC_QUERY_NAME} from '../../../zero-protocol/src/adhoc-queries.ts';
import {adhoc} from './adhoc.ts';
import {createBuilder} from './create-builder.ts';
import {asQueryInternals} from './query-internals.ts';
import {schema} from './test/test-schemas.ts';

const builder = createBuilder(schema);

describe('adhoc', () => {
  test('wraps a simple query with ADHOC_QUERY_NAME', () => {
    const query = builder.issue.where('id', '123');
    const adhocQuery = adhoc(query);

    const internals = asQueryInternals(adhocQuery);
    expect(internals.customQueryID).toEqual({
      name: ADHOC_QUERY_NAME,
      args: [{ast: asQueryInternals(query).ast}],
    });
  });

  test('preserves the original AST in the args', () => {
    const query = builder.issue
      .where('closed', true)
      .orderBy('createdAt', 'desc')
      .limit(10);
    const adhocQuery = adhoc(query);

    const internals = asQueryInternals(adhocQuery);
    const originalAst = asQueryInternals(query).ast;

    expect(internals.customQueryID?.args[0]).toEqual({ast: originalAst});
    expect(internals.ast).toEqual(originalAst);
  });

  test('works with queries that have related subqueries', () => {
    const query = builder.issue.related('owner').related('comments');
    const adhocQuery = adhoc(query);

    const internals = asQueryInternals(adhocQuery);
    expect(internals.customQueryID?.name).toBe(ADHOC_QUERY_NAME);
    expect(internals.ast.related).toHaveLength(2);
  });

  test('works with one() queries', () => {
    const query = builder.issue.where('id', '123').one();
    const adhocQuery = adhoc(query);

    const internals = asQueryInternals(adhocQuery);
    expect(internals.customQueryID?.name).toBe(ADHOC_QUERY_NAME);
    expect(internals.ast.limit).toBe(1);
  });

  test('ADHOC_QUERY_NAME constant is exported correctly', () => {
    expect(ADHOC_QUERY_NAME).toBe('_zero_adhoc');
  });

  test('adhoc query format is preserved', () => {
    const query = builder.issue.related('owner').one();
    const adhocQuery = adhoc(query);

    const internals = asQueryInternals(adhocQuery);
    expect(internals.format.singular).toBe(true);
    expect(internals.format.relationships).toHaveProperty('owner');
  });
});
