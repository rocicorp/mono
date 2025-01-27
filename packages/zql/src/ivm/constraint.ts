import {assert} from '../../../shared/src/asserts.ts';
import {stringCompare} from '../../../shared/src/string-compare.ts';
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
