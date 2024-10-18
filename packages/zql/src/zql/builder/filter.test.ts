import fc from 'fast-check';
import {expect, test} from 'vitest';
import type {
  SimpleCondition,
  SimpleOperator,
} from '../../../../zero-protocol/src/ast.js';
import {createPredicate} from './filter.js';
import {cases} from './like.test.js';

test('basics', () => {
  // nulls and undefined are false in all conditions
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
      fc.oneof(fc.hexaString(), fc.double(), fc.boolean()),
      (a, operator, b) => {
        const condition: SimpleCondition = {
          type: 'simple',
          field: 'foo',
          op: operator as SimpleOperator,
          value: b,
        };
        const predicate = createPredicate(condition);
        expect(predicate({foo: a})).toBe(false);
      },
    ),
  );

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
          field: 'foo',
          op: op as SimpleOperator,
          value: b,
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
        field: 'foo',
        op: flags ? 'ILIKE' : 'LIKE',
        value: pattern,
      };
      const predicate = createPredicate(condition);
      expect(predicate({foo: input})).toBe(expected);
    }
  }
});
