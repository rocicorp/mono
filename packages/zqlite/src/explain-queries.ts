import type {RowCountsBySource} from '../../zero-protocol/src/analyze-query-result.ts';
import type {Database} from './db.ts';

export function explainQueries(counts: RowCountsBySource, db: Database) {
  const plans: Record<string, string[]> = {};
  for (const querySet of Object.values(counts)) {
    const queries = Object.keys(querySet);
    for (const query of queries) {
      const plan = db
        .prepare(`EXPLAIN QUERY PLAN ${query}`)
        .explainQueryPlan()
        .map(r => (r as {detail: string}).detail);
      plans[query] = plan;
    }
  }

  return plans;
}
