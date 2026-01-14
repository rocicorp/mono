import {assert} from '../../../shared/src/asserts.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {
  Condition,
  SimpleCondition,
} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import {valuesEqual} from './data.ts';

export type Constraint = {
  readonly [key: string]: Value;
};

export function constraintMatchesRow(
  constraint: Constraint,
  row: Row,
): boolean {
  for (const key in constraint) {
    if (!valuesEqual(row[key], constraint[key])) {
      return false;
    }
  }
  return true;
}

/**
 * Constraints are compatible if:
 * 1. They do not have any keys in common
 * 2. They have keys in common, but the values for those keys are equal
 */
export function constraintsAreCompatible(
  left: Constraint,
  right: Constraint,
): boolean {
  for (const key in left) {
    if (key in right && !valuesEqual(left[key], right[key])) {
      return false;
    }
  }
  return true;
}

export function constraintMatchesPrimaryKey(
  constraint: Constraint,
  primary: PrimaryKey,
): boolean {
  return keyMatchesPrimaryKey(Object.keys(constraint), primary);
}

export function keyMatchesPrimaryKey(
  key: Iterable<string>,
  primary: PrimaryKey,
): boolean {
  const constraintKeys = [...key];

  if (constraintKeys.length !== primary.length) {
    return false;
  }

  // Primary key is always sorted
  // Constraint does not have to be sorted
  constraintKeys.sort(stringCompare);

  for (let i = 0; i < constraintKeys.length; i++) {
    if (constraintKeys[i] !== primary[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Pulls top level `and` components out of a condition tree.
 * The resulting array of simple conditions would match a superset of
 * values that the original condition would match.
 *
 * Examples:
 * a AND b OR c
 *
 * In this case we cannot pull anything because the `or` is at the top level.
 *
 * a AND b AND c
 * We can pull all three.
 *
 * a AND (b OR c)
 * We can only pull `a`.
 */
export function pullSimpleAndComponents(
  condition: Condition,
): SimpleCondition[] {
  if (condition.type === 'and') {
    return condition.conditions.flatMap(pullSimpleAndComponents);
  }

  if (condition.type === 'simple') {
    return [condition];
  }

  if (condition.type === 'or' && condition.conditions.length === 1) {
    return pullSimpleAndComponents(condition.conditions[0]);
  }

  return [];
}

/**
 * Checks if the supplied filters constitute a primary key lookup.
 * If so, returns the constraint that would be used to look up the primary key.
 * If not, returns undefined.
 */
export function primaryKeyConstraintFromFilters(
  condition: Condition | undefined,
  primary: PrimaryKey,
): Constraint | undefined {
  if (condition === undefined) {
    return undefined;
  }

  const conditions = pullSimpleAndComponents(condition);
  if (conditions.length === 0) {
    return undefined;
  }

  const ret: Writable<Constraint> = {};
  for (const subCondition of conditions) {
    if (subCondition.op === '=') {
      const column = extractColumn(subCondition);
      if (column !== undefined) {
        if (!primary.includes(column.name)) {
          continue;
        }
        ret[column.name] = column.value;
      }
    }
  }

  if (Object.keys(ret).length !== primary.length) {
    return undefined;
  }

  return ret;
}

function extractColumn(
  condition: SimpleCondition,
): {name: string; value: Value} | undefined {
  if (condition.left.type === 'column') {
    assert(
      condition.right.type === 'literal',
      () =>
        `extractColumn: expected right side to be literal, got ${condition.right.type}`,
    );
    return {name: condition.left.name, value: condition.right.value};
  }

  return undefined;
}

declare const TESTING: boolean;

export class SetOfConstraint {
  #data: Constraint[] = [];

  constructor() {
    // Only used in testing
    assert(TESTING);
  }

  #indexOf(value: Constraint): number {
    return this.#data.findIndex(v => constraintEquals(v, value));
  }

  has(value: Constraint): boolean {
    return this.#indexOf(value) !== -1;
  }

  add(value: Constraint): this {
    if (!this.has(value)) {
      this.#data.push(value);
    }
    return this;
  }
}

function constraintEquals(a: Constraint, b: Constraint): boolean {
  const aEntries = Object.entries(a);
  const bEntries = Object.entries(b);
  if (aEntries.length !== bEntries.length) {
    return false;
  }
  for (let i = 0; i < aEntries.length; i++) {
    if (
      aEntries[i][0] !== bEntries[i][0] ||
      !valuesEqual(aEntries[i][1], bEntries[i][1])
    ) {
      return false;
    }
  }
  return true;
}
