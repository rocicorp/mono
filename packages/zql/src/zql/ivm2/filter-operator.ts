import {assertNotNull, assertString} from 'shared/src/asserts.js';
import type {Filter, Input, Operator, Output, Request} from './operator.js';
import type {Change, TreeDiff} from './tree-diff.js';
import {deepEqual} from 'shared/src/json.js';
import {compareValues, type Value} from './data.js';
import type {SimpleOperator} from '../ast2/ast.js';

/**
 * FilterOperator is an operator that filters rows based on a predicate.
 *
 * Filter can currently only operate over the top-level `TreeDiff`, not
 * children. Our pipeline builder will always place `Filter`operators “above”
 * joins, which is more efficient since it reduces the number of rows that must
 * be joined.
 *
 * This may not be sufficient in the future, when we have subqueries in the
 * where position. We can expand filter to support filtering subdiffs if
 * necessary.
 */
export class FilterOperator implements Operator {
  readonly #input: Input;
  readonly #predicate: Filter;

  #output: Output | null = null;

  constructor(input: Input, predicate: Filter) {
    this.#input = input;
    this.#predicate = predicate;
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  push(_source: Input, diff: TreeDiff) {
    assertNotNull(this.#output);
    this.#output.push(this, {
      ...diff,
      changes: this.#apply(diff.changes),
    });
  }

  pull(req: Request) {
    const resp = this.#input.pull({
      ...req,
      optionalFilters: [...req.optionalFilters, this.#predicate],
    });

    // If the source was able to honor the filter, we don't have to redo
    // the work ourselves. Yay.
    // TODO: There could be a test that if appliedFilters are present,
    // FilterOperator doesn't do it itself.
    if (resp.appliedFilters.some(f => deepEqual(f, this.#predicate))) {
      return resp;
    }

    return {
      ...resp,
      diff: {
        ...resp.diff,
        changes: this.#apply(resp.diff.changes),
      },
    };
  }

  *#apply(changes: Iterable<Change>) {
    for (const change of changes) {
      // We can ignore nops because Filter doesn't support subdiffs.
      if (change.type === 'nop') {
        yield change;
      }

      const {field, op, value} = this.#predicate;
      const lhs = change.row[field];
      const rhs = value;

      if (matchesPredicate(lhs, op, rhs)) {
        yield change;
      }
    }
  }
}

function matchesPredicate(lhs: Value, op: SimpleOperator, rhs: Value): boolean {
  // TODO: Microbenchmark this case statement against returning a lambda and
  // calling it over and over.

  // TODO: What should be our policy on errors here?
  // We need to think about what happens when a pipeline fails. This
  // can happen if the db schema changes or doesn't match what dev
  // expected. We could toString() these puppies?
  switch (op) {
    case '=':
      return lhs === rhs;
    case '!=':
      return lhs !== rhs;
    case '<':
      return compareValues(lhs, rhs) < 0;
    case '>':
      return compareValues(lhs, rhs) > 0;
    case '<=':
      return lhs === rhs || compareValues(lhs, rhs) < 0;
    case '>=':
      return lhs === rhs || compareValues(lhs, rhs) > 0;
    case 'LIKE':
      assertString(lhs);
      assertString(rhs);
      return lhs.includes(rhs);
    default:
      throw new Error(`Unknown operator: ${op}`);
  }
}
