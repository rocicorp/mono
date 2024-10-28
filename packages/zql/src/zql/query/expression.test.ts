import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import {assert} from '../../../../shared/src/asserts.js';
import {and, or, type GenericCondition} from './expression.js';
import type {TableSchema} from './schema.js';

type TestCondition =
  | {
      type: 'simple';
      value: boolean;
    }
  | {
      type: 'and' | 'or';
      conditions: readonly TestCondition[];
    };

function simpleOr(l: TestCondition, r: TestCondition) {
  return {
    type: 'or',
    conditions: [l, r],
  } as const;
}

function simpleAnd(l: TestCondition, r: TestCondition) {
  return {
    type: 'and',
    conditions: [l, r],
  } as const;
}

function evaluate(condition: TestCondition): boolean {
  switch (condition.type) {
    case 'simple':
      return condition.value;
    case 'and':
      return condition.conditions.every(evaluate);
    case 'or':
      return condition.conditions.some(evaluate);
  }
}

describe('check the test framework', () => {
  test('simple', () => {
    expect(evaluate({type: 'simple', value: true})).toBe(true);
    expect(evaluate({type: 'simple', value: false})).toBe(false);
  });

  test('and', () => {
    expect(
      evaluate(
        simpleAnd({type: 'simple', value: true}, {type: 'simple', value: true}),
      ),
    ).toBe(true);
    expect(
      evaluate(
        simpleAnd(
          {type: 'simple', value: true},
          {type: 'simple', value: false},
        ),
      ),
    ).toBe(false);
    expect(
      evaluate(
        simpleAnd(
          {type: 'simple', value: false},
          {type: 'simple', value: true},
        ),
      ),
    ).toBe(false);
    expect(
      evaluate(
        simpleAnd(
          {type: 'simple', value: false},
          {type: 'simple', value: false},
        ),
      ),
    ).toBe(false);
  });

  test('or', () => {
    expect(
      evaluate(
        simpleOr({type: 'simple', value: true}, {type: 'simple', value: true}),
      ),
    ).toBe(true);
    expect(
      evaluate(
        simpleOr({type: 'simple', value: true}, {type: 'simple', value: false}),
      ),
    ).toBe(true);
    expect(
      evaluate(
        simpleOr({type: 'simple', value: false}, {type: 'simple', value: true}),
      ),
    ).toBe(true);
    expect(
      evaluate(
        simpleOr(
          {type: 'simple', value: false},
          {type: 'simple', value: false},
        ),
      ),
    ).toBe(false);
  });

  test('complex', () => {
    expect(
      evaluate(
        simpleOr(
          simpleAnd(
            {type: 'simple', value: true},
            {type: 'simple', value: true},
          ),
          simpleAnd(
            {type: 'simple', value: true},
            {type: 'simple', value: false},
          ),
        ),
      ),
    ).toBe(true);
  });
});

test('compare test framework to real framework', () => {
  // Generate a tree of TestConditions using fast-check
  fc.assert(
    fc.property(fc.integer({min: 1, max: 20}), numConditions => {
      const conditions: TestCondition[] = fc
        .sample(fc.boolean(), numConditions)
        .map(
          value =>
            ({
              type: 'simple',
              value,
            }) as const,
        );

      const pivots = conditions.map(
        () => fc.sample(fc.integer({min: 0, max: 100}), 1)[0] > 50,
      );

      const expected = conditions.reduce((acc, value, i) => {
        if (acc === undefined) {
          return value;
        }
        return pivots[i] ? simpleAnd(acc, value) : simpleOr(acc, value);
      });

      const actualConditions = conditions.map(convertTestCondition);
      const actual = actualConditions.reduce((acc, value, i) => {
        if (acc === undefined) {
          return value;
        }
        return pivots[i] ? and(value, acc) : or(value, acc);
      });

      expect(evaluate(actual as TestCondition)).toBe(evaluate(expected));

      // check that the real framework produced a DNF
      // console.log(toStr(actual));
      if (actual.type === 'and') {
        // all conditions should be simple as nothing can nest
        // under an `AND` in DNF
        expect(actual.conditions.every(c => c.type === 'simple')).toBe(true);
      } else if (actual.type === 'or') {
        // below an or can only be `ands` or `simple` conditions.
        expect(
          actual.conditions.every(c => c.type === 'and' || c.type === 'simple'),
        ).toBe(true);
        expect(
          actual.conditions
            .filter(c => c.type === 'and')
            .every(c =>
              // all conditions must be simple as nothing can nest
              // under an `AND` in DNF
              c.conditions.every(c => c.type === 'simple'),
            ),
        ).toBe(true);
      }
    }),
  );

  function convertTestCondition(
    c: TestCondition,
  ): GenericCondition<TableSchema> {
    assert(c.type === 'simple');
    return {
      type: 'simple',
      value: c.value,
      op: '=',
      field: 'n/a',
    };
  }
});

describe('flattening', () => {
  test('and chain', () => {
    expect(toStr(and(t(), t()))).toBe('(1 && 2)');
    id = 0;

    expect(toStr(and(t(), and(t(), and(t(), t()))))).toBe('(1 && 2 && 3 && 4)');
    id = 0;
  });

  test('and with nested ors', () => {
    expect(toStr(and(t(), or(t(), t())))).toBe('((2 && 1) || (3 && 1))');
    id = 0;

    expect(toStr(and(t(), or(t(), or(t(), t()))))).toBe(
      '((2 && 1) || (3 && 1) || (4 && 1))',
    );
    id = 0;

    expect(toStr(and(t(), and(t(), or(t(), and(t(), t())))))).toBe(
      '((3 && 2 && 1) || (4 && 5 && 2 && 1))',
    );
    id = 0;
  });
});

let id = 0;
function t() {
  return simple(++id);
}

function simple(value: number): GenericCondition<TableSchema> {
  return {type: 'simple', value, op: '=', field: 'n/a'};
}

function toStr(condition: GenericCondition<TableSchema>): string {
  switch (condition.type) {
    case 'simple':
      return condition.value.toString();
    case 'and':
      return `(${condition.conditions.map(toStr).join(' && ')})`;
    case 'or':
      return `(${condition.conditions.map(toStr).join(' || ')})`;
  }
}
