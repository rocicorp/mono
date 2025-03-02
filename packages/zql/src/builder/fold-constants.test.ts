import {expect, test} from 'vitest';
import {
  type Condition,
  type LiteralReference,
  type Parameter,
  type SimpleCondition,
  type ValuePosition,
} from '../../../zero-protocol/src/ast.ts';
import {FALSE, simplifySimple, TRUE} from './fold-constants.ts';
import fc from 'fast-check';

const param = fc.record({
  type: fc.constant('static'),
  anchor: fc.string(),
  field: fc.oneof(fc.string(), fc.array(fc.string())),
});

const column = fc.record({
  type: fc.constant('column'),
  name: fc.string(),
});

const nonNullVal = fc.oneof(fc.boolean(), fc.double(), fc.string());
const val = fc.oneof(fc.constant(null), nonNullVal);

const nonNullLiteral = fc.record({
  type: fc.constant('literal'),
  value: nonNullVal,
});
const literal = fc.record({
  type: fc.constant('literal'),
  value: val,
});

const isOp = fc.oneof(fc.constant('IS'), fc.constant('IS NOT'));
const nonIsOp = fc.oneof(
  fc.constant('='),
  fc.constant('!='),
  fc.constant('<'),
  fc.constant('>'),
  fc.constant('<='),
  fc.constant('>='),
);

const op = fc.oneof(isOp, nonIsOp);

test('simplifySimple', () => {
  // Comparing any non-literal a literal yields the original condition
  fc.assert(
    fc.property(fc.oneof(param, column), literal, op, (left, right, op) => {
      const condition: Condition = {
        type: 'simple',
        left: left as ValuePosition,
        right: right as LiteralReference,
        op: op as SimpleCondition['op'],
      };
      expect(simplifySimple(condition)).toEqual(condition);
    }),
  );

  // Comparing any literal to a non-literal yields the original condition
  fc.assert(
    fc.property(literal, fc.oneof(param), op, (left, right, op) => {
      const condition: Condition = {
        type: 'simple',
        left: left as LiteralReference,
        right: right as Parameter,
        op: op as SimpleCondition['op'],
      };
      expect(simplifySimple(condition)).toEqual(condition);
    }),
  );

  // Comparing null to anything else yields false
  fc.assert(
    fc.property(literal, nonIsOp, (lit, op) => {
      const c1: Condition = {
        type: 'simple',
        left: lit as LiteralReference,
        right: {type: 'literal', value: null},
        op: op as SimpleCondition['op'],
      };
      expect(simplifySimple(c1)).toEqual(FALSE);
      const c2: Condition = {
        type: 'simple',
        left: {type: 'literal', value: null},
        right: lit as LiteralReference,
        op: op as SimpleCondition['op'],
      };
      expect(simplifySimple(c2)).toEqual(FALSE);
    }),
  );

  // Comparing with IS
  fc.assert(
    fc.property(literal, literal, (left, right) => {
      const condition: Condition = {
        type: 'simple',
        left: left as LiteralReference,
        right: right as LiteralReference,
        op: 'IS',
      };
      expect(simplifySimple(condition)).toEqual(
        left.value === right.value ? TRUE : FALSE,
      );
    }),
  );

  // Comparing with IS NOT
  fc.assert(
    fc.property(literal, literal, (left, right) => {
      const condition: Condition = {
        type: 'simple',
        left: left as LiteralReference,
        right: right as LiteralReference,
        op: 'IS NOT',
      };
      expect(simplifySimple(condition)).toEqual(
        left.value !== right.value ? TRUE : FALSE,
      );
    }),
  );

  // Comparing with =
  fc.assert(
    fc.property(nonNullLiteral, nonNullLiteral, (left, right) => {
      const condition: Condition = {
        type: 'simple',
        left: left as LiteralReference,
        right: right as LiteralReference,
        op: '=',
      };
      expect(simplifySimple(condition)).toEqual(
        left.value === right.value ? TRUE : FALSE,
      );
    }),
  );

  // Comparing with !=
  fc.assert(
    fc.property(nonNullLiteral, nonNullLiteral, (left, right) => {
      const condition: Condition = {
        type: 'simple',
        left: left as LiteralReference,
        right: right as LiteralReference,
        op: '!=',
      };
      expect(simplifySimple(condition)).toEqual(
        left.value !== right.value ? TRUE : FALSE,
      );
    }),
  );

  // Comparing with <
  fc.assert(
    fc.property(fc.double(), fc.double(), (left, right) => {
      const condition: Condition = {
        type: 'simple',
        left: {
          type: 'literal',
          value: left,
        },
        right: {
          type: 'literal',
          value: right,
        },
        op: '<',
      };
      expect(simplifySimple(condition)).toEqual(left < right ? TRUE : FALSE);
    }),
  );
});
