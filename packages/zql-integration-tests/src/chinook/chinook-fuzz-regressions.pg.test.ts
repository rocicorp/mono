/* oxlint-disable no-console */

/**
 * **Corpus-first regression replay** (ported from rusty-ivm `rindle-fuzz`, design §9): every
 * committed minimized repro under `regressions/` is replayed against the Postgres oracle on
 * each run, so a once-found divergence can never silently return. A no-op until the first
 * regression is filed (see `regressions/README.md` for the format + filing workflow).
 */

import {test} from 'vitest';
import '../helpers/comparePg.ts';
import {bootstrap} from '../helpers/runner.ts';
import {checkRegressions, panicIfFailed} from './fuzz/driver.ts';
import {miniPgContent} from './fuzz/mini.ts';
import {loadRegressions, regressionsDir} from './fuzz/regressions.ts';
import {schema} from './schema.ts';

const TIMEOUT_MS = 120_000;

const harness = await bootstrap({
  suiteName: 'chinook_fuzz_regressions',
  zqlSchema: schema,
  pgContent: miniPgContent(),
});

const regressions = loadRegressions(regressionsDir());

// oxlint-disable-next-line expect-expect
test(
  'committed regressions replay parity-clean over mini',
  async () => {
    const report = await checkRegressions(
      harness.delegates,
      harness.transact,
      regressions,
    );
    console.log(
      `regressions: ${regressions.length} committed, ${report.failures.length} reproducing`,
    );
    panicIfFailed(report, 12);
  },
  TIMEOUT_MS,
);
