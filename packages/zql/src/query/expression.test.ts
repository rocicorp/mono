import fc from 'fast-check';
import {describe, expect, test, vi} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import {
  type Condition,
  type Conjunction,
  type Disjunction,
} from '../../../zero-protocol/src/ast.ts';
import {parse, stringify} from './expression-test-util.ts';
import {
  and,
  ExpressionBuilder,
  not,
  or,
  simplifyCondition,
} from './expression.ts';

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
      const actual = simplifyCondition(
        actualConditions.reduce((acc, value, i) => {
          if (acc === undefined) {
            return value;
          }
          return pivots[i] ? and(value, acc) : or(value, acc);
        }),
      );

      expect(evaluate(actual as TestCondition)).toBe(evaluate(expected));

      // check that the real framework produced a flattened condition
      // console.log(toStr(actual));
      const check = (c: Condition): boolean =>
        c.type === 'simple' ||
        c.type === 'correlatedSubquery' ||
        c.conditions.every(child => child.type !== c.type && check(child));
      expect(check(actual)).toBe(true);
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

test('bound methods/properties', () => {
  type Exists = ConstructorParameters<typeof ExpressionBuilder>[0];
  const mock = vi.fn<Exists>();
  const builder = new ExpressionBuilder(mock);

  const {eb, exists, cmp, cmpLit, and, or, not} = builder;

  expect(eb).toBe(builder);

  exists('a');
  expect(mock.mock.calls.length).toBe(1);
  expect(mock.mock.calls[0][0]).toBe('a');

  expect(cmp('a', '=', 'b')).toEqual({
    type: 'simple',
    left: {type: 'column', name: 'a'},
    right: {type: 'literal', value: 'b'},
    op: '=',
  });

  expect(cmpLit('a', '=', 'b')).toEqual({
    type: 'simple',
    left: {type: 'literal', value: 'a'},
    right: {type: 'literal', value: 'b'},
    op: '=',
  });

  expect(and(cmp('a', '=', 'b'), cmp('c', '=', 'd'))).toEqual({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 'b'},
        op: '=',
      },
      {
        type: 'simple',
        left: {type: 'column', name: 'c'},
        right: {type: 'literal', value: 'd'},
        op: '=',
      },
    ],
  });

  expect(or(cmp('a', '=', 'b'), cmp('c', '=', 'd'))).toEqual({
    type: 'or',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 'b'},
        op: '=',
      },
      {
        type: 'simple',
        left: {type: 'column', name: 'c'},
        right: {type: 'literal', value: 'd'},
        op: '=',
      },
    ],
  });

  expect(not(cmp('a', '=', 'b'))).toEqual({
    type: 'simple',
    left: {type: 'column', name: 'a'},
    right: {type: 'literal', value: 'b'},
    op: '!=',
  });
});

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

test('simplify', () => {
  const A = simple('A');
  const B = simple('B');
  const C = simple('C');
  const D = simple('D');
  const E = simple('E');

  const and = (...conditions: Condition[]): Conjunction => ({
    type: 'and',
    conditions,
  });
  const or = (...conditions: Condition[]): Disjunction => ({
    type: 'or',
    conditions,
  });
  expect(simplifyCondition(A)).toEqual(A);
  expect(simplifyCondition(and(A, B))).toEqual(and(A, B));
  expect(simplifyCondition(or(A, B))).toEqual(or(A, B));
  expect(simplifyCondition(and(or(A)))).toEqual(A);
  expect(simplifyCondition(or(and(or(A))))).toEqual(A);
  expect(simplifyCondition(and(A, or(B), or(or(C))))).toEqual(and(A, B, C));

  // A & (B & C) & (D | E) -> A & B & C & (D | E)
  expect(simplifyCondition(and(A, and(B, C), or(D, E)))).toEqual(
    and(A, B, C, or(D, E)),
  );

  // A | (B & C) | (D | E) -> A | B & C | D | E
  expect(simplifyCondition(or(A, and(B, C), or(D, E)))).toEqual(
    or(A, and(B, C), D, E),
  );
});
