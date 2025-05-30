import {assert} from '../../../shared/src/asserts.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import type {
  Condition,
  SimpleCondition,
} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from './data.ts';
import {valuesEqual} from './data.ts';

export type Constraint = {
  readonly [key: string]: Value;
};

export type PrimaryKey = readonly [string, ...string[]];

/**
 * When connecting to a source, an optional set of filters may
 * be specified.
 *
 * The source can take these filters into account in order
 * to perform index select during the `fetch` phase of
 * pipeline hydration. The source can also ignore all filters
 * if desired.
 */
export type SimpleOperator = EqualityOps | OrderOps | LikeOps | InOps;
export type EqualityOps = '=' | '!=' | 'IS' | 'IS NOT';
export type OrderOps = '<' | '>' | '<=' | '>=';
export type LikeOps = 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE';
export type InOps = 'IN' | 'NOT IN';
/**
 * As in SQL you can have multiple orderings. We don't currently
 * support ordering on anything other than the root query.
 */
export type OrderPart = readonly [field: string, direction: 'asc' | 'desc'];
export type Ordering = readonly OrderPart[];

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

export function constraintMatchesPrimaryKey(
  constraint: Constraint,
  primary: PrimaryKey,
): boolean {
  const constraintKeys = Object.keys(constraint);

  if (constraintKeys.length !== primary.length) {
    return false;
  }

  // Primary key is always sorted
  // Constraint does not have to be sorted
  constraintKeys.sort(stringCompare);

  for (let i = 0; i < constraintKeys.length; i++) {
    if (constraintKeys[i][0] !== primary[i]) {
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
    assert(condition.right.type === 'literal');
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
