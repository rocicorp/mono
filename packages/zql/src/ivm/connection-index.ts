import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {NoSubqueryCondition} from '../builder/filter.ts';
import {ChangeType} from './change-type.ts';
import {SourceChangeIndex} from './source-change-index.ts';
import type {SourceChange} from './source.ts';

/**
 * The static equality constraint a connection's filters place on a single
 * column: the row is accepted only if `row[column]` is one of `values`.
 * The push-time counterpart of the fetch-time `Constraint` in
 * `constraint.ts`, which pins columns to single values.
 */
export type StaticConstraint = {
  readonly column: string;
  readonly values: readonly Value[];
};

/**
 * Indexes the connections of a source by a static equality constraint
 * (`column = literal` or `column IN (literals)`) taken from each
 * connection's filters, so that a push can determine in O(distinct indexed
 * columns) whether any connection could possibly accept a row, before doing
 * any per-connection work.
 *
 * The index is conservative: {@link mayAccept} returns `true` whenever a
 * connection's filters could accept the row, and may return `true` when they
 * do not (e.g. the connection has further conditions that reject it).
 * It only ever returns `false` when every connection's filters reject the
 * row. Connections without a static equality constraint (no filters, or
 * filters whose top level is not an equality) count as unconstrained and
 * make every row a candidate, as does an index with no connections at all.
 *
 * Values are matched with SameValueZero (`Map` key semantics), which agrees
 * with the `=` / `IN` predicates (`===` / `Set#has`) for every value except
 * `NaN`, where the index is merely more permissive.
 */
export class ConnectionIndex<C> {
  // column -> value -> number of connections constrained to that value.
  readonly #byColumn = new Map<string, Map<Value, number>>();
  readonly #constraints = new Map<C, StaticConstraint | undefined>();
  #unconstrained = 0;

  add(connection: C, filters: NoSubqueryCondition | undefined): void {
    if (this.#constraints.has(connection)) {
      throw new Error('connection is already indexed');
    }
    const constraint = staticConstraint(filters);
    this.#constraints.set(connection, constraint);
    if (!constraint) {
      this.#unconstrained++;
      return;
    }
    if (constraint.values.length === 0) {
      // Never matches: indexed nowhere, so it is never a candidate.
      return;
    }
    let byValue = this.#byColumn.get(constraint.column);
    if (!byValue) {
      byValue = new Map();
      this.#byColumn.set(constraint.column, byValue);
    }
    for (const value of constraint.values) {
      byValue.set(value, (byValue.get(value) ?? 0) + 1);
    }
  }

  remove(connection: C): void {
    if (!this.#constraints.has(connection)) {
      throw new Error('connection is not indexed');
    }
    const constraint = this.#constraints.get(connection);
    this.#constraints.delete(connection);
    if (!constraint) {
      this.#unconstrained--;
      return;
    }
    if (constraint.values.length === 0) {
      return;
    }
    const byValue = this.#byColumn.get(constraint.column);
    if (!byValue) {
      throw new Error(`no index for column ${constraint.column}`);
    }
    for (const value of constraint.values) {
      const count = byValue.get(value) ?? 0;
      if (count <= 1) {
        byValue.delete(value);
      } else {
        byValue.set(value, count - 1);
      }
    }
    if (byValue.size === 0) {
      this.#byColumn.delete(constraint.column);
    }
  }

  get size(): number {
    return this.#constraints.size;
  }

  /**
   * Returns `false` only if the filters of every indexed connection reject
   * `row`. An empty index accepts every row: a source with no connections
   * is still written to (e.g. to populate it).
   */
  mayAccept(row: Row): boolean {
    if (this.#unconstrained > 0 || this.#constraints.size === 0) {
      return true;
    }
    for (const [column, byValue] of this.#byColumn) {
      if (byValue.has(row[column])) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns `false` only if the filters of every indexed connection reject
   * the row(s) of `change`: the row for an add or remove, and both the old
   * and the new row for an edit.
   */
  mayAcceptChange(change: SourceChange): boolean {
    if (this.mayAccept(change[SourceChangeIndex.ROW])) {
      return true;
    }
    return (
      change[SourceChangeIndex.TYPE] === ChangeType.EDIT &&
      this.mayAccept(change[SourceChangeIndex.OLD_ROW])
    );
  }
}

/**
 * Extracts a static equality constraint that every row accepted by
 * `condition` must satisfy, or `undefined` if there is none.
 *
 * - `column = literal`, `column IS literal` and `column IN (literals)`
 *   constrain the column. `column = null` (which the predicate folds to
 *   `false`) and an empty `IN` constrain it to the empty set, i.e. the
 *   connection never accepts a row.
 * - An `and` is constrained by any of its constrained conditions; the one
 *   with the fewest values is chosen, so a never-matching condition wins.
 * - An `or` is constrained only if all of its branches constrain the same
 *   column, by the union of their values. Never-matching branches cannot
 *   widen the result and are ignored.
 * - Anything else (`!=`, `IS NOT`, `NOT IN`, ranges, `LIKE`, a literal on
 *   the left) is unconstrained.
 */
export function staticConstraint(
  condition: NoSubqueryCondition | undefined,
): StaticConstraint | undefined {
  if (!condition) {
    return undefined;
  }
  switch (condition.type) {
    case 'simple': {
      const {left, right, op} = condition;
      if (left.type !== 'column' || right.type !== 'literal') {
        return undefined;
      }
      switch (op) {
        case '=':
          return {
            column: left.name,
            values: right.value === null ? [] : [right.value],
          };
        case 'IS':
          return {column: left.name, values: [right.value]};
        case 'IN':
          return Array.isArray(right.value)
            ? {column: left.name, values: right.value}
            : undefined;
        default:
          return undefined;
      }
    }
    case 'and': {
      let best: StaticConstraint | undefined;
      for (const c of condition.conditions) {
        const constraint = staticConstraint(c);
        if (
          constraint &&
          (!best || constraint.values.length < best.values.length)
        ) {
          best = constraint;
        }
      }
      return best;
    }
    case 'or': {
      let column: string | undefined;
      const values: Value[] = [];
      for (const c of condition.conditions) {
        const constraint = staticConstraint(c);
        if (!constraint) {
          return undefined;
        }
        if (constraint.values.length === 0) {
          continue; // a never-matching branch cannot widen the result
        }
        if (column !== undefined && constraint.column !== column) {
          return undefined;
        }
        column = constraint.column;
        values.push(...constraint.values);
      }
      // Every branch never matches: the whole `or` never matches.
      return {column: column ?? '', values};
    }
  }
}
