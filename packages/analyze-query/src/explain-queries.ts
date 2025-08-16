import type {RowCountsBySource} from '../../zql/src/builder/debug-delegate.ts';
import type {Database} from '../../zqlite/src/db.ts';

export function explainQueries(counts: RowCountsBySource, db: Database) {
  const plans: Record<string, string[]> = {};
  for (const querySet of Object.values(counts)) {
    const queries = Object.keys(querySet);
    for (const query of queries) {
      const plan = db
        // we should be more intelligent about value replacement.
        // Different values result in different plans. E.g., picking a value at the start
        // of an index will result in `scan` vs `search`. The scan is fine in that case.
        .prepare(`EXPLAIN QUERY PLAN ${query.replaceAll('?', "'sdfse'")}`)
        .all<{detail: string}>()
        .map(r => r.detail);
      plans[query] = plan;
    }
  }

  return plans;
}
