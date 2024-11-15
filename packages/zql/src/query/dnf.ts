import {unreachable} from '../../../shared/src/asserts.js';
import type {Condition, Disjunction} from '../../../zero-protocol/src/ast.js';
import {flatten, TRUE} from './expression.js';

/**
 * DNF (Disjunctive Normal Form) is a way to represent a boolean expression as a
 * series of `or` conditions where each `or` condition is a series of `and`
 * conditions. The `and` conditions are the terms (SimpleCondition) of the
 * expression.
 */
export function dnf(condition: Condition): Condition {
  return unwrap(dnfInner(condition));
}

function dnfInner(condition: Condition): Disjunction {
  switch (condition.type) {
    case 'simple':
    case 'correlatedSubquery':
      return {type: 'or', conditions: [condition]};

    case 'and':
      return distributeAnd(condition.conditions.map(dnfInner));

    case 'or':
      return {
        type: 'or',
        conditions: flatten(
          'or',
          condition.conditions.map(dnfInner).flatMap(c => c.conditions),
        ),
      };

    default:
      unreachable(condition);
  }
}

function distributeAnd(conditions: Disjunction[]): Disjunction {
  if (conditions.length === 0) {
    return {type: 'or', conditions: [TRUE]};
  }

  return conditions.reduce((acc, orCondition): Disjunction => {
    const newConditions: Condition[] = [];
    for (const accCondition of acc.conditions) {
      for (const orSubCondition of orCondition.conditions) {
        newConditions.push({
          type: 'and',
          conditions: [accCondition, orSubCondition],
        });
      }
    }
    return {
      type: 'or',
      conditions: flatten('or', newConditions),
    };
  });
}

export function unwrap(c: Condition): Condition {
  if (c.type === 'simple' || c.type === 'correlatedSubquery') {
    return c;
  }
  if (c.conditions.length === 1) {
    return unwrap(c.conditions[0]);
  }
  return {type: c.type, conditions: flatten(c.type, c.conditions.map(unwrap))};
}
