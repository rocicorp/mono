import {describe, expect, test} from 'vitest';
import {newQuery} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
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
