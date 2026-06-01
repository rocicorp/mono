/**
 * ArrayView transaction-batching benchmark.
 *
 * Measures applying K edits within a SINGLE transaction — K source pushes
 * followed by one commit (which flushes the view and fires listeners once) —
 * into a materialized list view, sweeping both the list width N and the
 * transaction size K.
 *
 * Why this matters: with the immutable `applyChange`, every push path-copies a
 * new spine from the root down to the changed row. A top-level edit copies the
 * entire top-level array via `array.with()` — O(N). Within one transaction only
 * the final state is ever observed (listeners fire once, on flush), so the K-1
 * intermediate arrays are pure allocation/GC churn. Total transaction cost is
 * therefore O(K * N) today.
 *
 * This is the guard/target for a future "mutate already-dirtied subtrees within
 * the open transaction" optimization: apply the first change immutably, then
 * mutate that fresh, not-yet-observed array in place for the rest of the
 * transaction → O(N + K). The N x K sweep makes the dependence on list width
 * (the per-edit copy) visible and gives a before/after to measure against.
 *
 * Note on regimes: when N is small the per-edit array copy is dwarfed by the
 * fixed per-change pipeline cost, so immutable ≈ mutable. The copy cost (and
 * thus the optimization's payoff) only dominates for wide lists, which is
 * exactly what the large-N rows below show.
 *
 * Uses only stable APIs (materialize / source push / commit), so it runs
 * unchanged on both the mutable baseline and the immutable applyChange, making
 * the two directly comparable.
 *
 * Run with:
 *   pnpm --filter zql-benchmarks run bench array-view-transaction
 */

import {bench, describe} from '../../shared/src/bench.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
} from '../../zql/src/ivm/source.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {builder, schema} from './schema.ts';

// Top-level list widths. The per-edit immutable array copy is O(N), so the
// transaction cost's dependence on N is what exposes the batching opportunity.
const LIST_WIDTHS = [1_000, 10_000];

// Number of edits applied inside a single transaction.
const K_VALUES = [1, 100, 1_000];

// ---- Data generation --------------------------------------------------------

type Dataset = {sources: Record<string, MemorySource>; issueRows: Row[]};

function makeFlatIssues(n: number): Dataset {
  const {tables} = schema;
  const sources: Record<string, MemorySource> = {};
  for (const [name, t] of Object.entries(tables)) {
    sources[name] = new MemorySource(t.name, t.columns, t.primaryKey);
  }
  const add = (tableName: string, row: Row) => {
    for (const _ of sources[tableName].push(makeSourceChangeAdd(row))) {
      /* consume */
    }
  };

  add('project', {
    id: 'proj-0',
    name: 'Project Zero',
    lowerCaseName: 'project zero',
  });

  // issueRows tracks the CURRENT value of each row so edits can be expressed as
  // makeSourceChangeEdit(next, current) (the source validates the old row); it
  // is updated in place as the benchmark edits.
  const issueRows: Row[] = [];
  for (let i = 0; i < n; i++) {
    const issue: Row = {
      id: `issue-${String(i).padStart(7, '0')}`,
      shortID: i,
      title: `Issue ${i}`,
      open: i % 3 !== 0,
      modified: 1_700_000_000_000 - i * 1000,
      created: 1_700_000_000_000 - i * 2000,
      projectID: 'proj-0',
      creatorID: 'u',
      assigneeID: undefined,
      description: `Description ${i}`,
      visibility: 'public',
    };
    issueRows.push(issue);
    add('issue', issue);
  }
  return {sources, issueRows};
}

/**
 * Returns a function that edits `count` distinct rows (cycling through `rows`),
 * changing a non-sort-key field (title) so positions stay stable and the edits
 * remain repeatable across samples without growing the dataset.
 */
function makeEditor(source: MemorySource, rows: Row[]) {
  let counter = 0;
  return (count: number) => {
    for (let j = 0; j < count; j++) {
      const idx = counter % rows.length;
      const current = rows[idx];
      const next = {...current, title: `v${counter}`};
      rows[idx] = next;
      counter++;
      for (const _ of source.push(makeSourceChangeEdit(next, current))) {
        /* consume */
      }
    }
  };
}

// Build one dataset per width up front.
const datasets = new Map<number, Dataset>(
  LIST_WIDTHS.map(n => [n, makeFlatIssues(n)]),
);

// Keep wall-clock per benchmark bounded: fewer samples as the work (K) grows.
const samplesForK = (k: number) => ({max_samples: Math.max(30, 10_000 / k)});

for (const n of LIST_WIDTHS) {
  const {sources, issueRows} = datasets.get(n)!;
  const edit = makeEditor(sources['issue'], issueRows);

  describe(`flat list N=${n}: K edits per transaction`, () => {
    for (const k of K_VALUES) {
      bench(
        `N=${n}: txn of ${k} edit(s)`,
        function* () {
          const delegate = new QueryDelegateImpl({sources});
          const view = delegate.materialize(builder.issue);
          const unlisten = view.addListener(() => {});
          yield () => {
            edit(k);
            delegate.commit();
          };
          unlisten();
          view.destroy();
        },
        samplesForK(k),
      );
    }
  });
}
