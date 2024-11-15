import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.js';
import {
  type Condition,
  type SimpleCondition,
} from '../../../zero-protocol/src/ast.js';
import {dnf} from './dnf.js';
import {parse, stringify} from './expression-test-util.js';
import {and, not, or} from './expression.js';

type TestCondition =
  | {
      type: 'simple';
      right: {
        value: boolean;
      };
    }
  | {
      type: 'and' | 'or';
      conditions: readonly TestCondition[];
    };

function simpleOr(...conditions: TestCondition[]): TestCondition {
  return {
    type: 'or',
    conditions,
  };
}

function simpleAnd(...conditions: TestCondition[]): TestCondition {
  return {
    type: 'and',
    conditions,
  };
}

function evaluate(condition: TestCondition): boolean {
  switch (condition.type) {
    case 'simple':
      return condition.right.value;
    case 'and':
      return condition.conditions.every(evaluate);
    case 'or':
      return condition.conditions.some(evaluate);
  }
}

describe('check the test framework', () => {
  test('simple', () => {
    expect(evaluate({type: 'simple', right: {value: true}})).toBe(true);
    expect(evaluate({type: 'simple', right: {value: false}})).toBe(false);
  });

  test('and', () => {
    expect(
      evaluate(
        simpleAnd(
          {type: 'simple', right: {value: true}},
          {type: 'simple', right: {value: true}},
        ),
      ),
    ).toBe(true);
    expect(
      evaluate(
        simpleAnd(
          {type: 'simple', right: {value: true}},
          {type: 'simple', right: {value: false}},
        ),
      ),
    ).toBe(false);
    expect(
      evaluate(
        simpleAnd(
          {type: 'simple', right: {value: false}},
          {type: 'simple', right: {value: true}},
        ),
      ),
    ).toBe(false);
    expect(
      evaluate(
        simpleAnd(
          {type: 'simple', right: {value: false}},
          {type: 'simple', right: {value: false}},
        ),
      ),
    ).toBe(false);
    expect(evaluate(simpleAnd({type: 'simple', right: {value: false}}))).toBe(
      false,
    );
    expect(evaluate(simpleAnd({type: 'simple', right: {value: true}}))).toBe(
      true,
    );
    expect(evaluate(simpleAnd())).toBe(true);
  });

  test('or', () => {
    expect(
      evaluate(
        simpleOr(
          {type: 'simple', right: {value: true}},
          {type: 'simple', right: {value: true}},
        ),
      ),
    ).toBe(true);
    expect(
      evaluate(
        simpleOr(
          {type: 'simple', right: {value: true}},
          {type: 'simple', right: {value: false}},
        ),
      ),
    ).toBe(true);
    expect(
      evaluate(
        simpleOr(
          {type: 'simple', right: {value: false}},
          {type: 'simple', right: {value: true}},
        ),
      ),
    ).toBe(true);
    expect(
      evaluate(
        simpleOr(
          {type: 'simple', right: {value: false}},
          {type: 'simple', right: {value: false}},
        ),
      ),
    ).toBe(false);
    expect(evaluate(simpleOr({type: 'simple', right: {value: false}}))).toBe(
      false,
    );
    expect(evaluate(simpleOr({type: 'simple', right: {value: true}}))).toBe(
      true,
    );
    expect(evaluate(simpleOr())).toBe(false);
  });

  test('complex', () => {
    expect(
      evaluate(
        simpleOr(
          simpleAnd(
            {type: 'simple', right: {value: true}},
            {type: 'simple', right: {value: true}},
          ),
          simpleAnd(
            {type: 'simple', right: {value: true}},
            {type: 'simple', right: {value: false}},
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
              right: {value},
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
      const actual = dnf(
        actualConditions.reduce((acc, value, i) => {
          if (acc === undefined) {
            return value;
          }
          return pivots[i] ? and(value, acc) : or(value, acc);
        }),
      );

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

  function convertTestCondition(c: TestCondition): Condition {
    assert(c.type === 'simple');
    return {
      type: 'simple',
      right: {
        type: 'literal',
        value: c.right.value,
      },
      op: '=',
      left: {
        type: 'column',
        name: 'n/a',
      },
    };
  }
});

describe('flattening', () => {
  let id = 0;
  function t(): SimpleCondition {
    return {
      type: 'simple',
      right: {
        type: 'literal',
        value: ++id,
      },
      op: '=',
      left: {
        type: 'column',
        name: 'n/a',
      },
    };
  }

  test('and chain', () => {
    expect(stringify(and(t(), t()))).toBe('1 & 2');
    id = 0;

    expect(stringify(dnf(and(t(), and(t(), and(t(), t())))))).toBe(
      '1 & 2 & 3 & 4',
    );
    id = 0;
  });

  test('and with nested ors', () => {
    expect(stringify(dnf(and(t(), or(t(), t()))))).toBe('(1 & 2) | (1 & 3)');
    id = 0;

    expect(stringify(dnf(and(t(), or(t(), or(t(), t())))))).toBe(
      '(1 & 2) | (1 & 3) | (1 & 4)',
    );
    id = 0;

    expect(stringify(dnf(and(t(), and(t(), or(t(), and(t(), t()))))))).toBe(
      '(1 & 2 & 3) | (1 & 2 & 4 & 5)',
    );
    id = 0;
  });
});

describe('simplify', () => {
  const FALSE: Condition = {type: 'or', conditions: []};
  const TRUE: Condition = {type: 'and', conditions: []};

  function simple(value: number | string): Condition {
    return {
      type: 'simple',
      right: {
        type: 'literal',
        value,
      },
      op: '=',
      left: {
        type: 'column',
        name: 'n/a',
      },
    };
  }

  const A = simple('A');
  const B = simple('B');

  test('simplify true/false in not', () => {
    expect(not(FALSE)).toEqual(TRUE);
    expect(not(TRUE)).toEqual(FALSE);
  });

  test('simplify true/false in and', () => {
    expect(and(FALSE, A)).toEqual(FALSE);
    expect(and(TRUE, A)).toEqual(A);
    expect(and(A, FALSE)).toEqual(FALSE);
    expect(and(A, TRUE)).toEqual(A);

    expect(and(FALSE, FALSE)).toEqual(FALSE);
    expect(and(TRUE, TRUE)).toEqual(TRUE);

    expect(and(or(A, B), TRUE)).toEqual(or(A, B));
  });

  test('simplify true/false in or', () => {
    expect(or(FALSE, A)).toEqual(A);
    expect(or(TRUE, A)).toEqual(TRUE);
    expect(or(A, FALSE)).toEqual(A);
    expect(or(A, TRUE)).toEqual(TRUE);

    expect(or(FALSE, FALSE)).toEqual(FALSE);
    expect(or(TRUE, TRUE)).toEqual(TRUE);

    expect(or(and(A, B), FALSE)).toEqual(and(A, B));
  });
});

test('not', () => {
  expect(stringify(not(parse('A = 1')))).toEqual('A != 1');
  expect(stringify(not(parse('A != 1')))).toEqual('A = 1');
  expect(stringify(not(parse('A < 1 & B > 2')))).toEqual('A >= 1 | B <= 2');
  expect(stringify(not(parse('A <= 1 | B >= 2')))).toEqual('A > 1 & B < 2');
  expect(stringify(not(parse('A IN abc')))).toEqual('A NOT IN abc');
  expect(stringify(not(parse('EXISTS () | NOT EXISTS ()')))).toEqual(
    'NOT EXISTS () & EXISTS ()',
  );
});
