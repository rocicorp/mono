// Exercises the analyze/debug path optimized by 257da8234: recording every
// vended row into one query bucket.

import {bench, describe, use} from '../../shared/src/bench.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {Debug} from '../../zql/src/builder/debug-delegate.ts';

const TABLE = 'issue';
const QUERY = 'SELECT * FROM issue WHERE projectID = ?';
const ROW_COUNTS = [1_000, 5_000, 10_000, 20_000];
const ROWS = Array.from<Row>({length: Math.max(...ROW_COUNTS)}, (_, i) => ({
  id: `issue-${i}`,
  projectID: 'proj-0',
  title: `Issue ${i}`,
  modified: 1_700_000_000_000 - i,
}));

describe('Debug.rowVended', () => {
  for (const rowCount of ROW_COUNTS) {
    bench(
      `record ${rowCount} rows in one query bucket`,
      () => {
        const debug = new Debug();
        debug.initQuery(TABLE, QUERY);

        for (let i = 0; i < rowCount; i++) {
          debug.rowVended(TABLE, QUERY, ROWS[i]);
        }

        const rows = debug.getVendedRows()[TABLE]?.[QUERY];
        if (rows?.length !== rowCount) {
          throw new Error(`Expected ${rowCount} vended rows`);
        }
        use(rows.length);
      },
      {
        max_samples: 50,
        min_cpu_time: 500_000_000,
        min_samples: 20,
      },
    );
  }
});
