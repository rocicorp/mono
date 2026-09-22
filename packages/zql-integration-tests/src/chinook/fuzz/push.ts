/**
 * **The four-phase push protocol** (ported from rusty-ivm `rindle-fuzz/src/push.rs`,
 * fuzzer-doc §6.1): generate a mutation history that drives the engine's incremental
 * maintenance, checked after *every* step (the driver's `pushWalk`).
 *
 * The four phases, applied phase-major over a chosen set of source rows:
 *
 * 1. **RemoveAll** — `remove(R)` for each row (membership loss);
 * 2. **AddBack** — `add(R)` for each (membership gain, restoring the seed);
 * 3. **EditToRandom** — `edit(R → R')` where `R'` is `R` with every NON-PK column set to
 *    a *different present value* (the PK fixed) — the highest-value mutation, moving the
 *    row across predicate / join / EXISTS-gate boundaries;
 * 4. **EditToMatch** — `edit(R' → R)`, restoring the seed.
 *
 * Each phase fully completes before the next, so the sequence is internally consistent
 * (every `remove`/`edit{old}` targets the current state) and ends at the original seed —
 * which lets a whole sweep share one rolled-back transaction.
 *
 * The push history targets the **root** (top-level membership churn) and the **deepest
 * leaf** (child re-parent / EXISTS gate open-close) — including a leaf that is *not* in
 * the output (an EXISTS-subquery table), so gate transitions are exercised.
 */

import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../../zero-protocol/src/data.ts';
import {columnsOf, pkOf} from './axes.ts';
import type {Data} from './literals.ts';
import {miniData} from './mini.ts';
import {deepestTable, type Skeleton} from './skeleton.ts';

/** A single mutation against a named source, in **client** names. */
export type Mutation =
  | {readonly table: string; readonly kind: 'remove'; readonly row: Row}
  | {readonly table: string; readonly kind: 'add'; readonly row: Row}
  | {
      readonly table: string;
      readonly kind: 'edit';
      readonly row: Row;
      readonly old: Row;
    };

/**
 * Complete `row` to the table's **full** column set, filling absent columns with `null`.
 * The seed rows authored in `miniData` omit always-null columns for brevity, but a
 * mutation must carry every column or the IVM source row (loaded full from PG) and the
 * oracle row would disagree on which keys are present (`null` vs absent) after an
 * add/edit — a spurious divergence.
 */
function completeRow(table: string, row: Row): Row {
  const out: Record<string, Value> = {};
  for (const col of columnsOf(table)) {
    out[col.name] = row[col.name] ?? null;
  }
  return out;
}

/**
 * `assignRandomValues` analogue: copy `row`, leaving the PK columns fixed and setting
 * every other column to a **different present value** of that column (pulled from the
 * data), so the edited row straddles a different set of predicate/join boundaries. A
 * column with no alternative present value is left unchanged.
 */
function randomize(data: Data, table: string, row: Row): Row {
  const pk = new Set(pkOf(table));
  const out: Record<string, Value> = {...row};
  for (const col of columnsOf(table)) {
    if (pk.has(col.name)) {
      continue;
    }
    const current = row[col.name];
    const diff = data.values(table, col.name).find(v => v !== current);
    if (diff !== undefined) {
      out[col.name] = diff;
    }
  }
  return out;
}

/**
 * The four-phase push sequence over the first `n` rows of `table`, with the rows pulled
 * **exactly** from the seed (so `remove`/`edit{old}` match by key and value). Empty if
 * the table has no seed rows.
 */
export function fourPhase(data: Data, table: string, n: number): Mutation[] {
  const rows = (miniData[table] ?? [])
    .slice(0, n)
    .map(r => completeRow(table, r));
  const edited = rows.map(r => randomize(data, table, r));
  const out: Mutation[] = [];
  for (const r of rows) {
    out.push({table, kind: 'remove', row: r}); // RemoveAll
  }
  for (const r of rows) {
    out.push({table, kind: 'add', row: r}); // AddBack
  }
  for (let i = 0; i < rows.length; i++) {
    out.push({table, kind: 'edit', row: edited[i], old: rows[i]}); // EditToRandom
  }
  for (let i = 0; i < rows.length; i++) {
    out.push({table, kind: 'edit', row: rows[i], old: edited[i]}); // EditToMatch
  }
  return out;
}

/**
 * The push history for a skeleton: four-phase on the **root** (top-level membership
 * churn) and on the **deepest leaf** (child re-parent / EXISTS gate), deduped. `n` rows
 * per table.
 */
export function pushForSkeleton(
  data: Data,
  skel: Skeleton,
  n: number,
): Mutation[] {
  const tables = [skel.table];
  const leaf = deepestTable(skel);
  if (leaf !== skel.table) {
    tables.push(leaf);
  }
  return tables.flatMap(t => fourPhase(data, t, n));
}

/**
 * The tables an AST touches (root + every correlated subquery / related child), so a
 * decorated case can be given mutations on both sides of a gate.
 */
export function queryTables(
  ast: AST,
  out: Set<string> = new Set(),
): Set<string> {
  out.add(ast.table);
  for (const r of ast.related ?? []) {
    queryTables(r.subquery, out);
  }
  const walk = (c: Condition | undefined): void => {
    if (!c) {
      return;
    }
    if (c.type === 'and' || c.type === 'or') {
      c.conditions.forEach(walk);
    } else if (c.type === 'correlatedSubquery') {
      queryTables(c.related.subquery, out);
    }
  };
  walk(ast.where);
  return out;
}

/**
 * The push history for a lowered skeleton: {@link pushForSkeleton}'s root and deepest
 * leaf first, then four-phase on **every other table** `ast` touches — so a table that
 * reaches the query through more than one connection (a self-join, two paths to one
 * table) is mutated, and one source change arrives at the same limited window twice.
 * The root + leaf prefix is kept because the sequence is state-dependent: it reaches
 * states the all-tables order alone does not.
 */
export function pushForQuery(
  data: Data,
  skel: Skeleton,
  ast: AST,
  n: number,
): Mutation[] {
  const tables = new Set([skel.table, deepestTable(skel), ...queryTables(ast)]);
  return [...tables].flatMap(t => fourPhase(data, t, n));
}
