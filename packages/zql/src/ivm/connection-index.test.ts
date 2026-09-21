import {describe, expect, test} from 'vitest';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {createPredicate, type NoSubqueryCondition} from '../builder/filter.ts';
import {ConnectionIndex, staticConstraint} from './connection-index.ts';
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

describe('staticConstraint', () => {
  test('no filters', () => {
    expect(staticConstraint(undefined)).toBeUndefined();
  });

  test('equality, IS and IN', () => {
    expect(staticConstraint(eq('a', 1))).toEqual({column: 'a', values: [1]});
    expect(staticConstraint(inn('a', [1, 2]))).toEqual({
      column: 'a',
      values: [1, 2],
    });
    expect(
      staticConstraint({
        type: 'simple',
        op: 'IS',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: null},
      }),
    ).toEqual({column: 'a', values: [null]});
  });

  test('never-matching conditions constrain to the empty set', () => {
    expect(staticConstraint(eq('a', null))).toEqual({column: 'a', values: []});
    expect(staticConstraint(inn('a', []))).toEqual({column: 'a', values: []});
    expect(
      staticConstraint({type: 'and', conditions: [eq('b', 1), eq('a', null)]}),
    ).toEqual({column: 'a', values: []});
    expect(
      staticConstraint({type: 'or', conditions: [eq('a', null), eq('b', 1)]}),
    ).toEqual({column: 'b', values: [1]});
    expect(
      staticConstraint({type: 'or', conditions: [eq('a', null), inn('b', [])]}),
    ).toEqual({column: '', values: []});
  });

  test('other operators and literal left sides are unconstrained', () => {
    expect(staticConstraint(gt('a', 1))).toBeUndefined();
    expect(
      staticConstraint({
        type: 'simple',
        op: '=',
        left: {type: 'literal', value: 1},
        right: {type: 'literal', value: 1},
      }),
    ).toBeUndefined();
    expect(
      staticConstraint({
        type: 'simple',
        op: '!=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 1},
      }),
    ).toBeUndefined();
  });

  test('and picks the constraint with the fewest values', () => {
    expect(
      staticConstraint({
        type: 'and',
        conditions: [gt('c', 0), inn('a', [1, 2, 3]), eq('b', 'x')],
      }),
    ).toEqual({column: 'b', values: ['x']});
    expect(
      staticConstraint({type: 'and', conditions: [gt('c', 0), gt('d', 1)]}),
    ).toBeUndefined();
  });

  test('or is constrained only when every branch constrains the same column', () => {
    expect(
      staticConstraint({
        type: 'or',
        conditions: [eq('a', 1), inn('a', [2, 3])],
      }),
    ).toEqual({column: 'a', values: [1, 2, 3]});
    expect(
      staticConstraint({
        type: 'or',
        conditions: [eq('a', 1), eq('b', 1)],
      }),
    ).toBeUndefined();
    expect(
      staticConstraint({
        type: 'or',
        conditions: [eq('a', 1), gt('a', 5)],
      }),
    ).toBeUndefined();
    expect(
      staticConstraint({
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

  test('IS NULL matches null cells; = NULL matches nothing', () => {
    const index = new ConnectionIndex<string>();
    index.add('c1', {
      type: 'simple',
      op: 'IS',
      left: {type: 'column', name: 'a'},
      right: {type: 'literal', value: null},
    });
    expect(index.mayAccept({a: null})).toBe(true);
    expect(index.mayAccept({a: 1})).toBe(false);
    index.remove('c1');
    index.add('c2', eq('a', null));
    expect(index.mayAccept({a: null})).toBe(false);
    expect(index.mayAccept({a: 1})).toBe(false);
    index.remove('c2');
    expect(index.size).toBe(0);
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

describe('soundness: a rejected row is rejected by every predicate', () => {
  // A tiny deterministic PRNG so failures are reproducible.
  let seed = 0x2f6e2b1;
  const rand = () => {
    seed ^= seed << 13;
    seed ^= seed >>> 17;
    seed ^= seed << 5;
    return (seed >>> 0) / 0x100000000;
  };
  const pick = <T>(xs: readonly T[]): T => xs[Math.floor(rand() * xs.length)];

  const columns = ['a', 'b', 'c'];
  const scalars: (string | number | boolean | null)[] = [
    0,
    1,
    2,
    'x',
    'y',
    true,
    false,
    null,
  ];
  const ops = ['=', '!=', 'IS', 'IS NOT', '<', '>', 'IN', 'NOT IN'] as const;

  function randomCondition(depth: number): NoSubqueryCondition {
    if (depth === 0 || rand() < 0.5) {
      const op = pick(ops);
      const column = pick(columns);
      const lit = () => pick(scalars);
      if (op === 'IN' || op === 'NOT IN') {
        const n = Math.floor(rand() * 3);
        const values: (string | number | boolean)[] = [];
        for (let i = 0; i < n; i++) {
          const v = lit();
          if (v !== null && v !== undefined) {
            values.push(v);
          }
        }
        return {
          type: 'simple',
          op,
          left: {type: 'column', name: column},
          right: {type: 'literal', value: values},
        };
      }
      if (op === '<' || op === '>') {
        return {
          type: 'simple',
          op,
          left: {type: 'column', name: column},
          right: {type: 'literal', value: pick([0, 1, 2])},
        };
      }
      return {
        type: 'simple',
        op,
        left: {type: 'column', name: column},
        right: {type: 'literal', value: lit() as string},
      };
    }
    const n = 1 + Math.floor(rand() * 3);
    const conditions: NoSubqueryCondition[] = [];
    for (let i = 0; i < n; i++) {
      conditions.push(randomCondition(depth - 1));
    }
    return {type: rand() < 0.5 ? 'and' : 'or', conditions};
  }

  function randomRow(): Row {
    const row: Record<string, Value> = {};
    for (const c of columns) {
      // Mostly numbers so range predicates do not throw on mixed types.
      row[c] = rand() < 0.8 ? pick([0, 1, 2]) : pick(scalars);
    }
    return row;
  }

  test('random conditions and rows', () => {
    for (let iter = 0; iter < 300; iter++) {
      const index = new ConnectionIndex<number>();
      const predicates: ((row: Row) => boolean)[] = [];
      const conditions: NoSubqueryCondition[] = [];
      const n = 1 + Math.floor(rand() * 5);
      for (let i = 0; i < n; i++) {
        const cond = randomCondition(2);
        conditions.push(cond);
        predicates.push(createPredicate(cond));
        index.add(i, cond);
      }
      for (let r = 0; r < 20; r++) {
        const row = randomRow();
        let accepted = false;
        let comparable = true;
        for (const p of predicates) {
          try {
            accepted ||= p(row);
          } catch {
            // Mixed-type range comparisons throw. Such a row could not be
            // pushed, so it says nothing about the index.
            comparable = false;
          }
        }
        if (comparable && accepted) {
          expect(
            index.mayAccept(row),
            `index rejected an accepted row ${JSON.stringify(row)} for ${JSON.stringify(conditions)}`,
          ).toBe(true);
        }
      }
    }
  });
});
