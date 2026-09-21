import {describe, expect, test} from 'vitest';
import type {NoSubqueryCondition} from '../builder/filter.ts';
import {ConnectionIndex, staticKey} from './connection-index.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from './source.ts';

function eq(column: string, value: unknown): NoSubqueryCondition {
  return {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: column},
    right: {type: 'literal', value: value as string},
  };
}

function inn(column: string, values: unknown[]): NoSubqueryCondition {
  return {
    type: 'simple',
    op: 'IN',
    left: {type: 'column', name: column},
    right: {type: 'literal', value: values as string[]},
  };
}

function gt(column: string, value: number): NoSubqueryCondition {
  return {
    type: 'simple',
    op: '>',
    left: {type: 'column', name: column},
    right: {type: 'literal', value},
  };
}

describe('staticKey', () => {
  test('no filters', () => {
    expect(staticKey(undefined)).toBeUndefined();
  });

  test('equality and IN', () => {
    expect(staticKey(eq('a', 1))).toEqual({column: 'a', values: [1]});
    expect(staticKey(eq('a', null))).toEqual({column: 'a', values: [null]});
    expect(staticKey(inn('a', [1, 2]))).toEqual({column: 'a', values: [1, 2]});
  });

  test('other operators and literal left sides are unconstrained', () => {
    expect(staticKey(gt('a', 1))).toBeUndefined();
    expect(
      staticKey({
        type: 'simple',
        op: '=',
        left: {type: 'literal', value: 1},
        right: {type: 'literal', value: 1},
      }),
    ).toBeUndefined();
    expect(
      staticKey({
        type: 'simple',
        op: '!=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 1},
      }),
    ).toBeUndefined();
  });

  test('and picks the constraint with the fewest values', () => {
    expect(
      staticKey({
        type: 'and',
        conditions: [gt('c', 0), inn('a', [1, 2, 3]), eq('b', 'x')],
      }),
    ).toEqual({column: 'b', values: ['x']});
    expect(
      staticKey({type: 'and', conditions: [gt('c', 0), gt('d', 1)]}),
    ).toBeUndefined();
  });

  test('or is constrained only when every branch constrains the same column', () => {
    expect(
      staticKey({
        type: 'or',
        conditions: [eq('a', 1), inn('a', [2, 3])],
      }),
    ).toEqual({column: 'a', values: [1, 2, 3]});
    expect(
      staticKey({
        type: 'or',
        conditions: [eq('a', 1), eq('b', 1)],
      }),
    ).toBeUndefined();
    expect(
      staticKey({
        type: 'or',
        conditions: [eq('a', 1), gt('a', 5)],
      }),
    ).toBeUndefined();
    expect(
      staticKey({
        type: 'or',
        conditions: [
          {type: 'and', conditions: [eq('a', 1), gt('c', 0)]},
          {type: 'and', conditions: [eq('a', 2), gt('d', 0)]},
        ],
      }),
    ).toEqual({column: 'a', values: [1, 2]});
  });
});

describe('ConnectionIndex', () => {
  test('empty index accepts everything', () => {
    const index = new ConnectionIndex<string>();
    expect(index.mayAccept({a: 1})).toBe(true);
    index.add('c1', eq('a', 1));
    expect(index.mayAccept({a: 2})).toBe(false);
    index.remove('c1');
    expect(index.mayAccept({a: 2})).toBe(true);
  });

  test('unconstrained connections accept everything', () => {
    const index = new ConnectionIndex<string>();
    index.add('c1', eq('a', 1));
    index.add('c2', undefined);
    expect(index.mayAccept({a: 2})).toBe(true);
    index.remove('c2');
    expect(index.mayAccept({a: 2})).toBe(false);
    expect(index.mayAccept({a: 1})).toBe(true);
  });

  test('constrained connections accept only their values', () => {
    const index = new ConnectionIndex<string>();
    index.add('c1', eq('a', 1));
    index.add('c2', eq('a', 1));
    index.add('c3', inn('b', ['x', 'y']));
    index.add('c4', {type: 'and', conditions: [eq('a', 3), eq('b', 'z')]});

    expect(index.mayAccept({a: 1, b: 'q'})).toBe(true);
    expect(index.mayAccept({a: 2, b: 'y'})).toBe(true);
    expect(index.mayAccept({a: 2, b: 'q'})).toBe(false);
    // Conservative: a matches c4's indexed column even though b does not.
    expect(index.mayAccept({a: 3, b: 'q'})).toBe(true);
    expect(index.mayAccept({a: '1', b: 'q'})).toBe(false);
    expect(index.mayAccept({b: 'x'})).toBe(true);
    expect(index.mayAccept({})).toBe(false);

    index.remove('c1');
    expect(index.mayAccept({a: 1, b: 'q'})).toBe(true);
    index.remove('c2');
    expect(index.mayAccept({a: 1, b: 'q'})).toBe(false);
    index.remove('c3');
    expect(index.mayAccept({a: 2, b: 'y'})).toBe(false);
    index.remove('c4');
    expect(index.size).toBe(0);
    // Empty again: permissive.
    expect(index.mayAccept({a: 3, b: 'z'})).toBe(true);
  });

  test('null values', () => {
    const index = new ConnectionIndex<string>();
    index.add('c1', eq('a', null));
    expect(index.mayAccept({a: null})).toBe(true);
    expect(index.mayAccept({a: 1})).toBe(false);
  });

  test('mayAcceptChange considers both rows of an edit', () => {
    const index = new ConnectionIndex<string>();
    index.add('c1', eq('a', 1));
    expect(index.mayAcceptChange(makeSourceChangeAdd({a: 1}))).toBe(true);
    expect(index.mayAcceptChange(makeSourceChangeAdd({a: 2}))).toBe(false);
    expect(index.mayAcceptChange(makeSourceChangeRemove({a: 1}))).toBe(true);
    expect(index.mayAcceptChange(makeSourceChangeRemove({a: 2}))).toBe(false);
    expect(index.mayAcceptChange(makeSourceChangeEdit({a: 1}, {a: 2}))).toBe(
      true,
    );
    expect(index.mayAcceptChange(makeSourceChangeEdit({a: 2}, {a: 1}))).toBe(
      true,
    );
    expect(index.mayAcceptChange(makeSourceChangeEdit({a: 2}, {a: 3}))).toBe(
      false,
    );
  });

  test('double add and unknown remove throw', () => {
    const index = new ConnectionIndex<string>();
    index.add('c1', undefined);
    expect(() => index.add('c1', undefined)).toThrow();
    expect(() => index.remove('c2')).toThrow();
  });
});
