/* oxlint-disable no-console */

/**
 * The **scalar-subquery lane** of the coverage-driven fuzzer — the one phase-7 axis deferred
 * at first. A `whereExists(rel, …, {scalar: true})` gate compiles to a different shape on
 * each side of the differential:
 *
 * - **Postgres (z2s)** compiles the original `scalar: true` AST to
 *   `parentField = (SELECT childField … LIMIT 1)`.
 * - **The IVM** does not natively resolve scalars; zero-cache's pipeline-driver pre-resolves
 *   *simple* (unique-key-constrained) ones to a literal `parentField = <value>` before
 *   hydration. The driver's `checkScalar` applies that exact transform
 *   ({@link resolveSimpleScalarSubqueries}) and runs the IVM over the resolved AST.
 *
 * So this is a true production mirror: original-through-z2s vs resolved-through-IVM must
 * agree. Every one-hop relationship in the schema yields one PK-constrained (hence simple)
 * scalar gate; a candidate that fails to resolve is reported (a generation regression), not
 * silently passed. Over the small, self-contained {@link miniPgContent mini} fixture, runs
 * per-PR.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {pkOf} from './fuzz/axes.ts';
import {checkScalar, panicIfFailed} from './fuzz/driver.ts';
import {Data} from './fuzz/literals.ts';
import {miniData, miniPgContent} from './fuzz/mini.ts';
import {scalarCandidates} from './fuzz/scalar.ts';
import {schema} from './schema.ts';

const TIMEOUT_MS = 120_000;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_scalar',
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

const data = new Data(miniData, pkOf);

test(
  'Scalar — simple scalar subqueries: resolved-IVM vs z2s parity over mini',
  async () => {
    const report = await checkScalar(harness.delegates, miniData, data);
    console.log(
      `Scalar: ${report.total} one-hop gates, ${report.failures.length} failures`,
    );
    // Every non-junction, single-col-PK-child relationship with a present child PK in mini
    // yields exactly one gate — a non-vacuous, exhaustive sweep over the relationship graph.
    const expected = scalarCandidates().filter(
      c => data.pkMid(c.child) !== undefined,
    ).length;
    expect(expected).toBeGreaterThan(0);
    expect(report.total).toBe(expected);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
