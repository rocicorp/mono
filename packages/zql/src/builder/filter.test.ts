import fc from 'fast-check';
import {expect, test} from 'vitest';
import type {SimpleCondition} from '../../../zero-protocol/src/ast.ts';
import {createPredicate} from './filter.ts';
import {cases} from './like-test-cases.ts';
import type {SimpleOperator} from '../ivm/constraint.ts';

test('basics', () => {
  // nulls and undefined are false in all conditions except IS NULL and IS NOT NULL
  fc.assert(
    fc.property(
      fc.oneof(fc.constant(null), fc.constant(undefined)),
      fc.oneof(
        fc.constant('='),
        fc.constant('!='),
        fc.constant('<'),
        fc.constant('<='),
        fc.constant('>'),
        fc.constant('>='),
        fc.constant('LIKE'),
        fc.constant('NOT LIKE'),
        fc.constant('ILIKE'),
        fc.constant('NOT ILIKE'),
      ),
      // hexastring to avoid sending escape chars to like
      fc.oneof(fc.hexaString(), fc.double(), fc.boolean(), fc.constant(null)),
      (a, operator, b) => {
        const condition: SimpleCondition = {
          type: 'simple',
          left: {
            type: 'column',
            name: 'foo',
          },
          op: operator as SimpleOperator,
          right: {
            type: 'literal',
            value: b,
          },
        };
        const predicate = createPredicate(condition);
        expect(predicate({foo: a})).toBe(false);
      },
    ),
  );

  let condition: SimpleCondition = {
    type: 'simple',
    left: {
      type: 'column',
      name: 'foo',
    },
    op: 'IS',
    right: {
      type: 'literal',
      value: null,
    },
  };
  let predicate = createPredicate(condition);
  expect(predicate({foo: null})).toBe(true);
  expect(predicate({foo: 1})).toBe(false);
  expect(predicate({foo: 'null'})).toBe(false);
  expect(predicate({foo: true})).toBe(false);
  expect(predicate({foo: false})).toBe(false);

  condition = {
    type: 'simple',
    left: {
      type: 'column',
      name: 'foo',
    },
    op: 'IS NOT',
    right: {
      type: 'literal',
      value: null,
    },
  };
  predicate = createPredicate(condition);
  expect(predicate({foo: null})).toBe(false);
  expect(predicate({foo: 1})).toBe(true);
  expect(predicate({foo: 'null'})).toBe(true);
  expect(predicate({foo: true})).toBe(true);
  expect(predicate({foo: false})).toBe(true);

  // basic operators
  fc.assert(
    fc.property(
      fc.oneof(fc.boolean(), fc.double(), fc.string()),
      fc.oneof(
        fc.constant('='),
        fc.constant('!='),
        fc.constant('<'),
        fc.constant('<='),
        fc.constant('>'),
        fc.constant('>='),
      ),
      fc.oneof(fc.boolean(), fc.double(), fc.string()),
      (a, op, b) => {
        const condition: SimpleCondition = {
          type: 'simple',
          left: {
            type: 'column',
            name: 'foo',
          },
          op: op as SimpleOperator,
          right: {
            type: 'literal',
            value: b,
          },
        };
        const predicate = createPredicate(condition);
        const jsOp = {'=': '===', '!=': '!=='}[op] ?? op;
        expect(predicate({foo: a})).toBe(eval(`a ${jsOp} b`));
      },
    ),
  );
});

test('like', () => {
  for (const {pattern, flags, inputs} of cases) {
    for (const [input, expected] of inputs) {
      const condition: SimpleCondition = {
        type: 'simple',
        left: {
          type: 'column',
          name: 'foo',
        },
        op: flags ? 'ILIKE' : 'LIKE',
        right: {
          type: 'literal',
          value: pattern,
        },
      };
      const predicate = createPredicate(condition);
      expect(predicate({foo: input})).toBe(expected);
    }
  }
});

test('and', () => {
  const predicate = createPredicate({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 4},
      },
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'b'},
        right: {type: 'literal', value: false},
      },
    ],
  });
  expect(predicate({a: 4, b: true})).false;
  expect(predicate({a: 3, b: false})).false;
  expect(predicate({a: 3, b: true})).false;
  expect(predicate({a: 4, b: false})).true;
});

test('or', () => {
  const predicate = createPredicate({
    type: 'or',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 4},
      },
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'b'},
        right: {type: 'literal', value: false},
      },
    ],
  });
  expect(predicate({a: 4, b: true})).true;
  expect(predicate({a: 3, b: false})).true;
  expect(predicate({a: 3, b: true})).false;
  expect(predicate({a: 4, b: false})).true;
});

test('empty and', () => {
  const predicate = createPredicate({
    type: 'and',
    conditions: [],
  });
  expect(predicate({a: 4, b: true})).true;
});

test('empty or', () => {
  const predicate = createPredicate({
    type: 'or',
    conditions: [],
  });
  expect(predicate({a: 4, b: true})).false;
});

test('nested', () => {
  const predicate = createPredicate({
    type: 'or',
    conditions: [
      {
        type: 'simple',
        op: '=',
        left: {type: 'column', name: 'a'},
        right: {type: 'literal', value: 4},
      },
      {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'a'},
            right: {type: 'literal', value: 3},
          },
          {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'b'},
            right: {type: 'literal', value: false},
          },
        ],
      },
    ],
  });
  expect(predicate({a: 4, b: true})).true;
  expect(predicate({a: 4, b: false})).true;
  expect(predicate({a: 3, b: false})).true;
  expect(predicate({a: 3, b: true})).false;
  expect(predicate({a: 5, b: false})).false;
});
