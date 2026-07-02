/**
 * **Metamorphic relations** (ported from rusty-ivm `rindle-fuzz/src/metamorphic.rs`,
 * design §6): semantically-invariant `AST` transforms that must not change the
 * materialized result. Checked **IVM-vs-IVM** (no oracle needed) — cheap extra coverage
 * on a *different* failure mode than the differential oracle: engine **self-inconsistency**
 * under a transform the answer is known to be invariant under.
 *
 * - **RedundantConjunct** — ANDing an always-true `pk IS NOT NULL` must not filter.
 * - **AndReorder** — reordering an `AND`'s conjuncts is invariant (AND is commutative).
 * - **LargeLimit** — a `limit` ≥ the result size is a no-op (a non-binding take).
 * - **StartBeforeFirst** — a `start` key below every PK (bare-PK sort) is a no-op (a
 *   non-binding skip). Guarded to single-column numeric PKs. This exercises the IVM's
 *   `start` handling directly — it is oracle-free, so z2s's lack of `start` compilation
 *   (the inert axis dropped from the differential generator) does not apply here.
 *
 * Each holds by query semantics, so a divergence is an engine bug found without a
 * hand-computed expectation.
 */

import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import {columnsOf, pkOf} from './axes.ts';
import {simple} from './literals.ts';

/** A semantically-invariant transform. */
export type Relation =
  | 'redundantConjunct'
  | 'andReorder'
  | 'largeLimit'
  | 'startBeforeFirst';

export const RELATIONS: readonly Relation[] = [
  'redundantConjunct',
  'andReorder',
  'largeLimit',
  'startBeforeFirst',
];

/**
 * Apply transform `r` to `ast`, or `null` if it does not apply (e.g. `andReorder` on a
 * non-`AND` `where`, `largeLimit` when a limit is already present). Pure — never mutates
 * `ast`.
 */
export function transform(ast: AST, r: Relation): AST | null {
  switch (r) {
    case 'redundantConjunct': {
      // AND `pk IS NOT NULL` (always true — PK columns are non-null) onto the root.
      const pk0 = pkOf(ast.table)[0];
      if (!pk0) {
        return null;
      }
      const conds: Condition[] = !ast.where
        ? []
        : ast.where.type === 'and'
          ? [...ast.where.conditions]
          : [ast.where];
      conds.push(simple(pk0, 'IS NOT', null));
      return {...ast, where: foldAnd(conds)};
    }
    case 'andReorder': {
      if (ast.where?.type !== 'and' || ast.where.conditions.length < 2) {
        return null;
      }
      return {
        ...ast,
        where: {type: 'and', conditions: ast.where.conditions.toReversed()},
      };
    }
    case 'largeLimit': {
      if (ast.limit !== undefined) {
        return null;
      }
      return {...ast, limit: 100_000}; // ≥ the whole fixture ⇒ non-binding
    }
    case 'startBeforeFirst': {
      // Only meaningful when the sort is the bare PK (no `order_by`), there is no existing
      // bound, and the PK is a single numeric column (a clearly-below-everything key).
      if (
        (ast.orderBy && ast.orderBy.length > 0) ||
        ast.start !== undefined ||
        !singleNumericPk(ast.table)
      ) {
        return null;
      }
      const pk0 = pkOf(ast.table)[0];
      return {...ast, start: {row: {[pk0]: -1_000_000}, exclusive: false}};
    }
  }
}

/** Whether `table`'s primary key is a single numeric column. */
function singleNumericPk(table: string): boolean {
  const pk = pkOf(table);
  if (pk.length !== 1) {
    return false;
  }
  const col = columnsOf(table).find(c => c.name === pk[0]);
  return col?.type === 'number';
}

function foldAnd(conds: readonly Condition[]): Condition | undefined {
  if (conds.length === 0) {
    return undefined;
  }
  if (conds.length === 1) {
    return conds[0];
  }
  return {type: 'and', conditions: conds};
}
