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
