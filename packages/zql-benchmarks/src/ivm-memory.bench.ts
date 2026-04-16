/**
 * Standalone IVM benchmarks that run entirely in-memory (no PostgreSQL needed).
 *
 * These benchmarks exercise the hot paths of the IVM pipeline:
 * - Hydration (initial fetch)
 * - Push (incremental update)
 *
 * Run with:
 *   npm --workspace=zql-benchmarks run bench:memory
 */

import {bench, describe} from '../../shared/src/bench.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {builder, schema} from './schema.ts';

import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../zql/src/ivm/source.ts';
// ---- Data sizes -------------------------------------------------------------
// Keep small to run fast; increase for more stable results.

const NUM_USERS = 50;
const NUM_ISSUES = 500;
const NUM_COMMENTS = 1000;
const NUM_LABELS = 10;
const NUM_ISSUE_LABELS = 500;

// ---- Data generation --------------------------------------------------------

function makeSources() {
  const {tables} = schema;

  const sources: Record<string, MemorySource> = {};
  for (const [name, tableSchema] of Object.entries(tables)) {
    sources[name] = new MemorySource(
      tableSchema.name,
      tableSchema.columns,
      tableSchema.primaryKey,
    );
  }

  function add(tableName: string, row: Row) {
    for (const _ of sources[tableName].push(makeSourceChangeAdd(row))) {
      /* consume */
    }
  }

  // Users
  for (let i = 0; i < NUM_USERS; i++) {
    add('user', {
      id: `user-${i}`,
      login: `user${i}`,
      name: `User ${i}`,
      avatar: `avatar${i}`,
      role: i % 10 === 0 ? 'crew' : 'user',
    });
  }

  // Projects
  add('project', {
    id: 'proj-0',
    name: 'Project Zero',
    lowerCaseName: 'project zero',
  });
  add('project', {
    id: 'proj-1',
    name: 'Project One',
    lowerCaseName: 'project one',
  });

  // Issues
  const issues: Row[] = [];
  for (let i = 0; i < NUM_ISSUES; i++) {
    const row: Row = {
      id: `issue-${i}`,
      shortID: i,
      title: `Issue ${i}: Some bug or feature request`,
      open: i % 3 !== 0,
      modified: 1_700_000_000_000 - i * 1000,
      created: 1_700_000_000_000 - i * 2000,
      projectID: `proj-${i % 2}`,
      creatorID: `user-${i % NUM_USERS}`,
      assigneeID: i % 4 === 0 ? undefined : `user-${(i + 1) % NUM_USERS}`,
      description: `Description for issue ${i}`,
      visibility: i % 5 === 0 ? 'internal' : 'public',
    };
    issues.push(row);
    add('issue', row);
  }

  // Comments
  for (let i = 0; i < NUM_COMMENTS; i++) {
    add('comment', {
      id: `comment-${i}`,
      issueID: `issue-${i % NUM_ISSUES}`,
      created: 1_700_000_000_000 - i * 500,
      body: `Comment body ${i}`,
      creatorID: `user-${i % NUM_USERS}`,
    });
  }

  // Labels
  for (let i = 0; i < NUM_LABELS; i++) {
    add('label', {
      id: `label-${i}`,
      name: `label-${i}`,
      projectID: `proj-${i % 2}`,
    });
  }

  // Issue labels (unique pairs)
  const seen = new Set<string>();
  let il = 0;
  for (let i = 0; il < NUM_ISSUE_LABELS; i++) {
    const issueID = `issue-${i % NUM_ISSUES}`;
    const labelID = `label-${i % NUM_LABELS}`;
    const key = `${issueID}|${labelID}`;
    if (!seen.has(key)) {
      seen.add(key);
      add('issueLabel', {
        issueID,
        labelID,
        projectID: `proj-${i % 2}`,
      });
      il++;
    }
  }

  return {sources, issues};
}

// Build dataset once
const {sources, issues} = makeSources();

// ---- Hydration benchmarks ---------------------------------------------------

// Each iteration creates a full IVM pipeline over the entire dataset. Limit
// max_samples so delegates don't accumulate faster than GC can collect them.
const hydrationOpts = {max_samples: 100};

describe('hydration', () => {
  bench(
    'hydrate: issues only',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(builder.issue);
    },
    hydrationOpts,
  );

  bench(
    'hydrate: issues with creator',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(builder.issue.related('creator'));
    },
    hydrationOpts,
  );

  bench(
    'hydrate: issues with creator + comments',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(builder.issue.related('creator').related('comments'));
    },
    hydrationOpts,
  );

  bench(
    'hydrate: issues filtered open',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(builder.issue.where('open', true));
    },
    hydrationOpts,
  );

  bench(
    'hydrate: issues limit 50',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(builder.issue.limit(50));
    },
    hydrationOpts,
  );
});

// ---- Push benchmarks --------------------------------------------------------

// Generator-style benches: setup runs once, the yielded fn runs max_samples
// times, then cleanup runs. Without a cap the source accumulates ~666k rows
// (2s / 3µs) before removeAll is called, exhausting heap.
const pushOpts = {max_samples: 1_000};

function addAndTrack(source: MemorySource, row: Row, addedRows: Row[]): void {
  addedRows.push(row);
  for (const _ of source.push(makeSourceChangeAdd(row))) {
    /* consume */
  }
}

function removeAll(source: MemorySource, rows: Row[]): void {
  for (const row of rows) {
    for (const _ of source.push(makeSourceChangeRemove(row))) {
      /* consume */
    }
  }
}

describe('push', () => {
  let pushCount = 0;

  bench(
    'push: add issue (no join)',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(builder.issue);
      const addedRows: Row[] = [];

      yield () => {
        addAndTrack(
          sources['issue'],
          {
            id: `push-issue-${pushCount++}`,
            shortID: NUM_ISSUES + pushCount,
            title: `Push Issue ${pushCount}`,
            open: true,
            modified: Date.now(),
            created: Date.now(),
            projectID: 'proj-0',
            creatorID: 'user-0',
            assigneeID: undefined,
            description: 'Pushed issue',
            visibility: 'public',
          },
          addedRows,
        );
      };

      removeAll(sources['issue'], addedRows);
      view.destroy();
    },
    pushOpts,
  );

  bench(
    'push: add issue (with creator join)',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(builder.issue.related('creator'));
      const addedRows: Row[] = [];

      yield () => {
        addAndTrack(
          sources['issue'],
          {
            id: `push-issue-j-${pushCount++}`,
            shortID: NUM_ISSUES + pushCount,
            title: `Push Issue ${pushCount}`,
            open: true,
            modified: Date.now(),
            created: Date.now(),
            projectID: 'proj-0',
            creatorID: 'user-0',
            assigneeID: undefined,
            description: 'Pushed issue',
            visibility: 'public',
          },
          addedRows,
        );
      };

      removeAll(sources['issue'], addedRows);
      view.destroy();
    },
    pushOpts,
  );

  bench(
    'push: edit issue title',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(builder.issue);
      let editCount = 0;

      yield () => {
        const idx = editCount % NUM_ISSUES;
        const oldRow = issues[idx];
        const newRow = {...oldRow, title: `Edited ${editCount++}`};
        for (const _ of sources['issue'].push(
          makeSourceChangeEdit(newRow, oldRow as Row),
        )) {
          /* consume */
        }
        // restore
        for (const _ of sources['issue'].push(
          makeSourceChangeEdit(oldRow as Row, newRow),
        )) {
          /* consume */
        }
      };

      view.destroy();
    },
    pushOpts,
  );

  bench(
    'push: add comment (child relation)',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(builder.issue.related('comments'));
      const addedRows: Row[] = [];

      yield () => {
        addAndTrack(
          sources['comment'],
          {
            id: `push-comment-${pushCount++}`,
            issueID: `issue-${pushCount % NUM_ISSUES}`,
            created: Date.now(),
            body: 'A new comment',
            creatorID: 'user-0',
          },
          addedRows,
        );
      };

      removeAll(sources['comment'], addedRows);
      view.destroy();
    },
    pushOpts,
  );

  bench(
    'push: add issue inside limit(50)',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(builder.issue.limit(50));
      const addedRows: Row[] = [];

      yield () => {
        // Very low ID ensures insertion at front (inside limit)
        addAndTrack(
          sources['issue'],
          {
            id: `aaa-${String(pushCount++).padStart(12, '0')}`,
            shortID: -pushCount,
            title: `Front issue ${pushCount}`,
            open: true,
            modified: Date.now(),
            created: Date.now(),
            projectID: 'proj-0',
            creatorID: 'user-0',
            assigneeID: undefined,
            description: 'Front of list',
            visibility: 'public',
          },
          addedRows,
        );
      };

      removeAll(sources['issue'], addedRows);
      view.destroy();
    },
    pushOpts,
  );

  bench(
    'push: add issue outside limit(50)',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(builder.issue.limit(50));
      const addedRows: Row[] = [];

      yield () => {
        // Very high ID ensures insertion past limit
        addAndTrack(
          sources['issue'],
          {
            id: `zzz-${pushCount++}`,
            shortID: NUM_ISSUES + pushCount,
            title: `Back issue ${pushCount}`,
            open: true,
            modified: Date.now(),
            created: Date.now(),
            projectID: 'proj-0',
            creatorID: 'user-0',
            assigneeID: undefined,
            description: 'End of list',
            visibility: 'public',
          },
          addedRows,
        );
      };

      removeAll(sources['issue'], addedRows);
      view.destroy();
    },
    pushOpts,
  );
});
