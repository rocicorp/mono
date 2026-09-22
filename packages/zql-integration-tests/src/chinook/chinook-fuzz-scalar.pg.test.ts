/* oxlint-disable no-console */

/**
 * The **scalar-subquery lane** of the coverage-driven fuzzer.
 *
 * `{scalar: true}` on an EXISTS gate is a plan hint, not a semantic one: it asserts the
 * subquery matches at most one row so an engine *may* decorrelate the gate. Every engine in
 * this differential is free to ignore it — z2s does (Postgres decorrelates EXISTS on its
 * own) and the base IVM does; only zero-cache's pipeline-driver acts on it, pre-resolving a
 * *simple* (unique-key-pinned) subquery, and that transform is unit-tested directly in
 * `zqlite/src/resolve-scalar-subqueries.test.ts`.
 *
 * So the property here is **scalar-invariance**: marking gates scalar must not change
 * results. Every `2^k` scalar assignment of an EXISTS-bearing skeleton is pinned to the same
 * Postgres oracle, so any assignment that diverges surfaces.
 *
 * This replaces a lane that generated only PK-pinned gates and ran the *resolved* AST
 * through the IVM against the *original* through z2s. That mirrored production but assumed
 * the rewrite valid on both sides, so it could not observe a wrong one — and it missed a z2s
 * bug that dropped rows for every scalar gate not pinned to a unique key. Skeleton gates are
 * undecorated, hence unpinned, so that space is now the lane's default.
 *
 * Runs per-PR over the small, self-contained {@link miniPgContent mini} fixture.
 */

import {expect, test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {
  checkScalarInvariance,
  panicIfFailed,
  scalarQueryCases,
} from './fuzz/driver.ts';
import {miniPgContent} from './fuzz/mini.ts';
import {enumerate} from './fuzz/skeleton.ts';
import {schema} from './schema.ts';

const TIMEOUT_MS = 120_000;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_scalar',
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

test(
  'Scalar-invariance — every scalar plan of an EXISTS query hydrate-equal over mini',
  async () => {
    // D≤2 so a scalar gate can nest a further EXISTS — the shape of the reported bug
    // (`src/scalar-nested-exists.pg.test.ts`), and one the old candidate list could not
    // express. Deeper scalar×scalar nesting rides the nightly sweep.
    const skels = enumerate({depth: 2, related: 1, exists: 2});
    // Non-vacuous: the corpus must actually contain markable gates.
    const cases = scalarQueryCases(skels);
    expect(cases.length).toBeGreaterThan(0);

    const report = await checkScalarInvariance(harness.delegates, skels);
    console.log(
      `Scalar: ${report.total} scalar-variants, ${report.failures.length} failures`,
    );
    expect(report.total).toBe(cases.length);
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
