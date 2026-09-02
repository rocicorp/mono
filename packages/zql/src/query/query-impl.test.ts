import {describe, expect, test} from 'vitest';
import {newQuery, newQueryImpl} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
import type {AnyQuery} from './query.ts';
import {newStaticQuery} from './static-query.ts';
import {schema} from './test/test-schemas.ts';

describe('QueryImpl run/preload/materialize', () => {
  test('run() throws on non-runnable query', () => {
    const issueQuery = newQuery(schema, 'issue');
    expect(() => issueQuery.run()).toThrow('Query is not runnable');
  });

  test('preload() throws on non-runnable query', () => {
    const issueQuery = newQuery(schema, 'issue');
    expect(() => issueQuery.preload()).toThrow('Query is not runnable');
  });

  test('materialize() throws on non-runnable query', () => {
    const issueQuery = newQuery(schema, 'issue');
    expect(() => issueQuery.materialize()).toThrow('Query is not runnable');
  });

  test('run() throws on chained non-runnable query', () => {
    const issueQuery = newQuery(schema, 'issue')
      .where('id', '0001')
      .related('owner')
      .orderBy('id', 'asc')
      .limit(10);
    expect(() => issueQuery.run()).toThrow('Query is not runnable');
  });

  test('preload() throws on chained non-runnable query', () => {
    const issueQuery = newQuery(schema, 'issue')
      .where('id', '0001')
      .related('owner')
      .orderBy('id', 'asc')
      .limit(10);
    expect(() => issueQuery.preload()).toThrow('Query is not runnable');
  });

  test('materialize() throws on chained non-runnable query', () => {
    const issueQuery = newQuery(schema, 'issue')
      .where('id', '0001')
      .related('owner')
      .orderBy('id', 'asc')
      .limit(10);
    expect(() => issueQuery.materialize()).toThrow('Query is not runnable');
  });

  test('one() on non-runnable query still throws on run()', () => {
    const issueQuery = newQuery(schema, 'issue').one();
    expect(() => issueQuery.run()).toThrow('Query is not runnable');
  });
});

describe('QueryImpl.hash covers custom query identity', () => {
  const base = newQuery(schema, 'issue').where('closed', false);
  const bi = asQueryInternals(base);

  test('a named query differs from its unnamed base', () => {
    // `nameAndArgs` leaves the AST untouched, so the hash has to pick the name
    // and args up from somewhere else or these two are indistinguishable.
    expect(asQueryInternals(bi.nameAndArgs('foo', [1])).hash()).not.toBe(
      bi.hash(),
    );
  });

  test('different names differ', () => {
    expect(asQueryInternals(bi.nameAndArgs('foo', [1])).hash()).not.toBe(
      asQueryInternals(bi.nameAndArgs('bar', [1])).hash(),
    );
  });

  test('different args differ', () => {
    expect(asQueryInternals(bi.nameAndArgs('foo', [1])).hash()).not.toBe(
      asQueryInternals(bi.nameAndArgs('foo', [2])).hash(),
    );
  });

  test('the same name and args agree', () => {
    expect(asQueryInternals(bi.nameAndArgs('foo', [1])).hash()).toBe(
      asQueryInternals(bi.nameAndArgs('foo', [1])).hash(),
    );
  });
});

describe('QueryImpl.hash covers the system', () => {
  test('a client query differs from a permissions query over the same table', () => {
    // With no relationship and no `exists`, there is nowhere in the AST for the
    // system to be stamped, so the hash has to carry it.
    expect(asQueryInternals(newQuery(schema, 'issue')).hash()).not.toBe(
      asQueryInternals(newStaticQuery(schema, 'issue')).hash(),
    );
  });

  test('the same system agrees', () => {
    expect(asQueryInternals(newQuery(schema, 'issue')).hash()).toBe(
      asQueryInternals(newQuery(schema, 'issue')).hash(),
    );
  });
});

describe('QueryImpl.hash covers the junction', () => {
  // The sub-query handed to a two-hop callback carries the junction, which
  // decides whether `limit` and `orderBy` throw but leaves no trace in the AST.
  // Rebuilding a query from that sub-query's own AST and format gives one that
  // is identical in every way the AST can see, so only the junction is left to
  // tell them apart.
  function capture(
    build: (cb: (q: AnyQuery) => AnyQuery) => unknown,
  ): AnyQuery {
    let captured: AnyQuery | undefined;
    build(q => {
      captured = q;
      return q;
    });
    expect(captured).toBeDefined();
    return captured!;
  }

  function withoutJunction(q: AnyQuery): AnyQuery {
    const {ast, format} = asQueryInternals(q);
    return newQueryImpl(
      schema,
      ast.table as keyof typeof schema.tables,
      ast,
      format,
      'client',
    ) as AnyQuery;
  }

  test('the sub-query of a two-hop related differs from the same query without the junction', () => {
    const inner = capture(cb =>
      newQuery(schema, 'issue').related('labels', cb),
    );
    expect(() => inner.limit(1)).toThrow('Limit is not supported');
    expect(asQueryInternals(inner).hash()).not.toBe(
      asQueryInternals(withoutJunction(inner)).hash(),
    );
  });

  test('the sub-query of a two-hop exists differs from the same query without the junction', () => {
    const inner = capture(cb =>
      newQuery(schema, 'issue').whereExists('labels', cb),
    );
    expect(() => inner.orderBy('id', 'asc')).toThrow(
      'Order by is not supported',
    );
    expect(asQueryInternals(inner).hash()).not.toBe(
      asQueryInternals(withoutJunction(inner)).hash(),
    );
  });

  test('the sub-query of a one-hop related has no junction', () => {
    const inner = capture(cb => newQuery(schema, 'issue').related('owner', cb));
    expect(asQueryInternals(inner).hash()).toBe(
      asQueryInternals(withoutJunction(inner)).hash(),
    );
  });

  test('the same junction agrees', () => {
    const a = capture(cb => newQuery(schema, 'issue').related('labels', cb));
    const b = capture(cb => newQuery(schema, 'issue').related('labels', cb));
    expect(asQueryInternals(a).hash()).toBe(asQueryInternals(b).hash());
  });
});
