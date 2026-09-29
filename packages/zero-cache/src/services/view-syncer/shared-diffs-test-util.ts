import type {LogContext} from '@rocicorp/logger';
import {onTestFinished} from 'vitest';
import type {AppID} from '../../types/shards.ts';
import {SharedDiffs} from './shared-diffs.ts';

const byReplica = new Map<string, SharedDiffs>();

/**
 * The {@link SharedDiffs} for the Snapshotters of a test on the replica at
 * `dbFile`, as selected by `ZERO_TEST_SHARED_DIFFS`: none if unset or `0`,
 * and if `1`, one per replica per test, as there is one per sync worker.
 */
export function testSharedDiffs(
  lc: LogContext,
  dbFile: string,
  appID: AppID,
): SharedDiffs | undefined {
  const mode = process.env['ZERO_TEST_SHARED_DIFFS'];
  switch (mode) {
    case undefined:
    case '':
    case '0':
      return undefined;
    case '1':
      break;
    default:
      throw new Error(`Unknown ZERO_TEST_SHARED_DIFFS: ${mode}`);
  }
  let shared = byReplica.get(dbFile);
  if (shared === undefined) {
    const created = new SharedDiffs(lc, dbFile, appID, {
      maxBytes: 64 * 1024 * 1024,
    });
    byReplica.set(dbFile, created);
    onTestFinished(() => {
      byReplica.delete(dbFile);
      created.destroy();
    });
    shared = created;
  }
  return shared;
}
