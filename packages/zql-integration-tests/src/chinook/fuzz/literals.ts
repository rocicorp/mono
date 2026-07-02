/**
 * **Non-vacuous, data-driven values** (ported from rusty-ivm `rindle-fuzz/src/literals.rs`,
 * design §5). Random literals over a fixed fixture mostly produce empty results
 * (coverage of an empty tree ≈ 0). Two tactics:
 *
 * - **Pull constants from the data.** {@link Data} indexes the actual column values in a
 *   fixture, so a `start` bound (and later mutation/random literals) is a *real* present
 *   value rather than a guess.
 * - **Reuse the tuned role hints.** {@link filterCondition} builds a root `where` filter
 *   from a table's {@link Roles} — literals already hand-tuned to the `mini` data so the
 *   filter *partitions* the rows.
 *
 * The filter `Condition` is hand-built as raw AST (no correlation keys needed); EXISTS
 * conditions, which DO need correlation keys + junction handling, are built through the
 * fluent expression builder in `cover.ts`.
 */

import type {
  Condition,
  LiteralValue,
  Ordering,
  SimpleCondition,
  SimpleOperator,
} from '../../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../../zero-protocol/src/data.ts';
import {type FilterVal, type Roles, rolesOf} from './axes.ts';

/** `column <op> literal` — the only simple-condition shape the generator emits. */
export function simple(
  col: string,
  op: SimpleOperator,
  value: LiteralValue,
): SimpleCondition {
  return {
    type: 'simple',
    op,
    left: {type: 'column', name: col},
    right: {type: 'literal', value},
  };
}

/** Compare two non-null fixture values (numbers numerically, otherwise by string). */
function compareValues(a: Value, b: Value): number {
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }
  return String(a) < String(b) ? -1 : String(a) > String(b) ? 1 : 0;
}

/**
 * An index of the **actual** values present in a fixture, for data-driven literal
 * selection. Keyed by `table` → `column` → the sorted, distinct, non-null values.
 */
export class Data {
  readonly #cols = new Map<string, Value[]>();
  readonly #pk0 = new Map<string, Value[]>();
  readonly #rows = new Map<string, readonly Row[]>();
  readonly #pkOf: (table: string) => readonly string[];

  /** Index the column values of every table in `data` (keyed by client table name). */
  constructor(
    data: Record<string, Row[]>,
    pkOf: (table: string) => readonly string[],
  ) {
    this.#pkOf = pkOf;
    for (const [table, rows] of Object.entries(data)) {
      this.#rows.set(table, rows);
      const columns = new Set<string>();
      for (const row of rows) {
        for (const c of Object.keys(row)) {
          columns.add(c);
        }
      }
      for (const c of columns) {
        this.#cols.set(`${table}.${c}`, distinctSorted(rows.map(r => r[c])));
      }
      const pk0 = pkOf(table)[0];
      if (pk0 !== undefined) {
        this.#pk0.set(table, distinctSorted(rows.map(r => r[pk0])));
      }
    }
  }

  /** The sorted distinct non-null values of `table.col`, if any. */
  values(table: string, col: string): readonly Value[] {
    return this.#cols.get(`${table}.${col}`) ?? [];
  }

  /**
   * A **present** median value of the table's first PK column — the keyset value for a
   * non-vacuous `start` bound (keeps ≈ half the rows). `undefined` if no rows.
   */
  pkMid(table: string): Value | undefined {
    const vals = this.#pk0.get(table);
    if (!vals || vals.length === 0) {
      return undefined;
    }
    return vals[Math.floor(vals.length / 2)];
  }

  /**
   * A present row near the middle of `table` under `orderBy` completed with PK columns.
   * This gives `start` a non-vacuous cursor that reflects an actual fixture row.
   */
  startRow(table: string, orderBy: Ordering | undefined): Row | undefined {
    const rows = this.#rows.get(table);
    if (!rows || rows.length === 0) {
      return undefined;
    }
    const ordering = completeOrdering(this.#pkOf(table), orderBy);
    const sorted = rows.toSorted((a, b) => compareRows(a, b, ordering));
    return sorted[Math.floor(sorted.length / 2)];
  }

  /** A null cursor on the first completed ordering term, for null-bound paging cases. */
  nullStartRow(table: string, orderBy: Ordering | undefined): Row | undefined {
    const first = completeOrdering(this.#pkOf(table), orderBy)[0]?.[0];
    return first ? {[first]: null} : undefined;
  }
}

function distinctSorted(values: readonly (Value | undefined)[]): Value[] {
  const nonNull = values.filter(
    (v): v is Value => v !== null && v !== undefined,
  );
  nonNull.sort(compareValues);
  const out: Value[] = [];
  for (const v of nonNull) {
    const last = out.at(-1);
    if (last === undefined || compareValues(last, v) !== 0) {
      out.push(v);
    }
  }
  return out;
}

function completeOrdering(
  primaryKey: readonly string[],
  orderBy: Ordering | undefined,
): Ordering {
  const completed = [...(orderBy ?? [])];
  const seen = new Set(completed.map(([field]) => field));
  for (const field of primaryKey) {
    if (!seen.has(field)) {
      completed.push([field, 'asc']);
    }
  }
  return completed;
}

function compareRows(a: Row, b: Row, orderBy: Ordering): number {
  for (const [field, direction] of orderBy) {
    const comp = compareValues(a[field], b[field]);
    if (comp !== 0) {
      return direction === 'asc' ? comp : -comp;
    }
  }
  return 0;
}

// ── filter lowering (the `filter` axis → a root `where` condition) ───────────────────

/**
 * Build the root `where` condition for a {@link FilterVal} on `table`, using its tuned
 * value roles. `null` ⇒ no filter (`'none'`) **or** the value needs a text column the
 * table lacks (callers gate on `filterRealizable` first, so in practice only `'none'`).
 */
export function filterCondition(table: string, v: FilterVal): Condition | null {
  const r: Roles = rolesOf(table);
  const num = r.num;
  const mid = r.numMid;
  const like = (p: string): string => `${p}%`;
  switch (v) {
    case 'none':
      return null;
    case 'eq':
      return simple(num, '=', mid);
    case 'ne':
      return simple(num, '!=', mid);
    case 'lt':
      return simple(num, '<', mid);
    case 'le':
      return simple(num, '<=', mid);
    case 'gt':
      return simple(num, '>', mid);
    case 'ge':
      return simple(num, '>=', mid);
    case 'in':
      return simple(num, 'IN', [r.inSet[0], r.inSet[1]]);
    case 'is_null':
      return simple(r.nullable, 'IS', null);
    case 'is_not_null':
      return simple(r.nullable, 'IS NOT', null);
    case 'like':
      return r.text ? simple(r.text, 'LIKE', like(r.likePrefix)) : null;
    case 'ilike':
      return r.text ? simple(r.text, 'ILIKE', like(r.ilikePrefix)) : null;
    case 'not_like':
      return r.text ? simple(r.text, 'NOT LIKE', like(r.likePrefix)) : null;
    case 'and2':
      return r.text
        ? {
            type: 'and',
            conditions: [
              simple(num, '!=', mid),
              simple(r.text, 'LIKE', like(r.likePrefix)),
            ],
          }
        : null;
    case 'or2':
      return {
        type: 'or',
        conditions: [simple(num, '=', mid), simple(num, '>', mid)],
      };
    case 'and_or':
      return r.text
        ? {
            type: 'and',
            conditions: [
              simple(num, '!=', r.inSet[0]),
              {
                type: 'or',
                conditions: [
                  simple(num, '=', mid),
                  simple(r.text, 'LIKE', like(r.likePrefix)),
                ],
              },
            ],
          }
        : null;
  }
}
