import {describe, expect, test} from 'vitest';
import {newQuery} from './query-impl.ts';
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
