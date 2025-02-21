import {assert} from '../../../shared/src/asserts.ts';
import type {
  AST,
  Condition,
  LiteralValue,
  ValuePosition,
} from '../../../zero-protocol/src/ast.ts';

export function foldConstants(ast: AST): AST {
  if (!ast.where) {
    return ast;
  }
  return {
    ...ast,
    where: simplifyCondition(ast.where),
  };
}

function simplifyCondition(condition: Condition): Condition {
  switch (condition.type) {
    case 'and':
      return simplifyAnd({
        ...condition,
        conditions: condition.conditions.map(simplifyCondition),
      });
    case 'or':
      return simplifyOr({
        ...condition,
        conditions: condition.conditions.map(simplifyCondition),
      });
    case 'simple':
      return simplifySimple(condition);
    case 'correlatedSubquery':
      return {
        ...condition,
        related: {
          ...condition.related,
          subquery: foldConstants(condition.related.subquery),
        },
      };
  }
}

function isTRUE(condition: Condition): boolean {
  return condition.type === 'and' && condition.conditions.length === 0;
}

function isFALSE(condition: Condition): boolean {
  return condition.type === 'or' && condition.conditions.length === 0;
}

const TRUE: Condition = {
  type: 'and',
  conditions: [],
};

const FALSE: Condition = {
  type: 'or',
  conditions: [],
};

function simplifyAnd(condition: Condition): Condition {
  assert(condition.type === 'and');
  if (condition.conditions.some(isFALSE)) {
    return FALSE;
  }
  if (condition.conditions.every(isTRUE)) {
    return TRUE;
  }
  return condition;
}

function simplifyOr(condition: Condition): Condition {
  assert(condition.type === 'or');
  if (condition.conditions.some(isTRUE)) {
    return TRUE;
  }
  if (condition.conditions.every(isFALSE)) {
    return FALSE;
  }
  return condition;
}

function simplifySimple(condition: Condition): Condition {
  assert(condition.type === 'simple');
  const left = getLiteralValue(condition.left);
  const right = getLiteralValue(condition.right);
  if (left === undefined || right === undefined) {
    return condition;
  }
  const value = evaluate(condition.op, left, right);
  if (value === undefined) {
    return condition;
  }
  return value ? TRUE : FALSE;
}

function getLiteralValue(value: ValuePosition): LiteralValue | undefined {
  if (value.type === 'literal') {
    return value.value;
  }
  return undefined;
}

function evaluate(
  op: string,
  left: LiteralValue,
  right: LiteralValue,
): boolean | undefined {
  throw new Error('TODO');
}
