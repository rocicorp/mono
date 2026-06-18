/**
 * **Auto-filed regression repros** (ported from rusty-ivm `rindle-fuzz/src/regress.rs`,
 * design §7/§9). When the fuzzer finds a divergence, the shrinker's minimized case is
 * serialized to JSON under `regressions/`, committed, and **replayed first** on every run
 * — so each past find becomes a permanent guard against silent reintroduction.
 *
 * The mono fixture is fixed (the deterministic {@link miniData mini} fixture), so — unlike
 * the Rust port — a regression need not carry its sources: it is just the minimized `AST`
 * (the JSON wire format) plus an optional four-phase push history (client-named {@link
 * Mutation}s, also plain JSON) and a human note. Replay re-wraps the AST and routes to the
 * same differential check it was filed under (hydrate, or per-step push).
 *
 * To file one: serialize the shrunk case (the driver's `minimizeRepro` gives the AST) with
 * {@link serializeRegression} and write it to {@link regressionsDir} as `*.json`. The
 * replay test (`chinook-fuzz-regressions.pg.test.ts`) then guards it forever.
 */

import {readFileSync, readdirSync} from 'node:fs';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Mutation} from './push.ts';

/** A committed regression: a minimized case + how it was found. */
export type Regression = {
  /** A human description (what bug this guards). */
  readonly note: string;
  /** The minimized query AST (the JSON wire format). */
  readonly ast: AST;
  /** An optional four-phase push history (client names); absent ⇒ a hydrate-only repro. */
  readonly pushes?: readonly Mutation[];
};

/** Pretty-print a regression for committing under {@link regressionsDir}. */
export function serializeRegression(reg: Regression): string {
  return JSON.stringify(reg, null, 2);
}

/** Parse a committed regression (the inverse of {@link serializeRegression}). */
export function parseRegression(json: string): Regression {
  return JSON.parse(json) as Regression;
}

/** The committed regressions directory (`src/chinook/regressions/`). */
export function regressionsDir(): string {
  return fileURLToPath(new URL('../regressions/', import.meta.url));
}

/**
 * Load every regression in `dir`, in deterministic (filename) order. A missing directory
 * yields none; a corrupt file is skipped (so one bad commit can't break the lane).
 */
export function loadRegressions(dir: string): Regression[] {
  let names: string[];
  try {
    names = readdirSync(dir);
  } catch {
    return [];
  }
  const out: Regression[] = [];
  for (const name of names.filter(n => n.endsWith('.json')).sort()) {
    try {
      out.push(parseRegression(readFileSync(join(dir, name), 'utf-8')));
    } catch {
      // skip a corrupt / partially-written regression file
    }
  }
  return out;
}
