/**
 * **Cost control** (ported from rusty-ivm `rindle-fuzz/src/cost.rs`, design §3) — the
 * "long-running query" problem. The L0/L1/push backbone is bounded by construction; this
 * gate mainly protects the L4 random tail (deeper nesting over the larger fixtures).
 *
 * **Static cost gate (pre-flight, {@link CostModel}).** Estimate
 * `cost ≈ Π_levels(expected_fanout) × (1 + Σ_exists |table|)` from the fixture table
 * sizes; if it exceeds a threshold, the driver **skips and counts** the case rather than
 * running it. Cheap, no engine change.
 *
 * The Rust port also carries an in-flight wall-clock watchdog (`run_with_deadline`), run
 * on a detached worker thread. We omit it here: JS is single-threaded and the IVM has no
 * cooperative-cancellation primitive, so a true hang cannot be abandoned mid-`materialize`
 * anyway — the static gate over the bounded `mini` fixture plus vitest's per-test timeout
 * are the backstop. (Revisit with a `Promise.race` deadline if the nightly chinook-scale
 * subset — a later phase — needs to abandon a slow PG round-trip.)
 */

import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../../zero-protocol/src/data.ts';

/** A static query-cost estimator over per-table row counts (client table names). */
export class CostModel {
  readonly #sizes: ReadonlyMap<string, number>;
  /** Skip + count a query whose estimate exceeds this. */
  readonly threshold: number;

  constructor(sizes: ReadonlyMap<string, number>, threshold: number) {
    this.#sizes = sizes;
    this.threshold = threshold;
  }

  /** Build from a fixture's actual table sizes (`miniData`-shaped: client rows). */
  static fromData(data: Record<string, Row[]>, threshold: number): CostModel {
    const sizes = new Map<string, number>();
    for (const [table, rows] of Object.entries(data)) {
      sizes.set(table, rows.length);
    }
    return new CostModel(sizes, threshold);
  }

  /** Build from explicit `(table, rows)` sizes (chinook-scale estimates, or a synthetic
   * large fixture for testing the gate). */
  static fromSizes(
    sizes: Iterable<readonly [string, number]>,
    threshold: number,
  ): CostModel {
    return new CostModel(new Map(sizes), threshold);
  }

  #size(table: string): number {
    return Math.max(1, this.#sizes.get(table) ?? 1);
  }

  /**
   * The estimated cost: the product of table sizes down each materialized nesting chain,
   * times one plus the sum of EXISTS table sizes. (JS numbers do not wrap; a pathological
   * estimate simply grows past the threshold — which is all the gate needs.)
   */
  estimate(ast: AST): number {
    let product = this.#size(ast.table);
    for (const r of ast.related ?? []) {
      product *= Math.max(1, this.estimate(r.subquery));
    }
    const exists = ast.where ? this.#condExistsSum(ast.where) : 0;
    return product * (1 + exists);
  }

  #condExistsSum(c: Condition): number {
    switch (c.type) {
      case 'simple':
        return 0;
      case 'and':
      case 'or':
        return c.conditions.reduce((s, cc) => s + this.#condExistsSum(cc), 0);
      case 'correlatedSubquery': {
        const sub = c.related.subquery;
        const nested = sub.where ? this.#condExistsSum(sub.where) : 0;
        return this.#size(sub.table) + nested;
      }
    }
  }

  /** Whether `ast` should be skipped (cost over threshold). */
  tooExpensive(ast: AST): boolean {
    return this.estimate(ast) > this.threshold;
  }
}
