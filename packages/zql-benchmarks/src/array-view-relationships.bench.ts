/**
 * ArrayView benchmarks for relationship-heavy hydration and push.
 *
 * These target the scenario that regressed in #5462 ("make applyChange
 * immutable for React.memo optimization") and was reverted in #5616:
 *
 *   - Cold-load hydration of a query with wide + deep relationships
 *     materialized into an ArrayView. This is the path that froze the page
 *     for ~13s on initial sync. The reverted code eagerly walked the entire
 *     relationship tree (`expandNode`/`expandChange`) on every hydrated node.
 *   - A long series of pushes into a relationship-heavy view. This exercises
 *     the "new spine per push" / GC-churn question raised when discussing the
 *     reland: an immutable applyChange that path-copies on every push must not
 *     blow up allocation for big push series.
 *
 * The benchmarks only use stable APIs (`delegate.run` for hydration,
 * `delegate.materialize` + source pushes for incremental updates), so the same
 * file establishes a baseline on the current (mutable) code and re-runs
 * unchanged after the immutable applyChange is re-landed, making the two
 * directly comparable.
 *
 * Run with:
 *   pnpm --filter zql-benchmarks run bench array-view-relationships
 */

import {bench, describe} from '../../shared/src/bench.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from '../../zql/src/ivm/source.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {builder, schema} from './schema.ts';

// ---- Data sizes -------------------------------------------------------------
// Sized so relationships are genuinely wide/deep (the regression amplifier) but
// still fast to run. The reverted code's cost scaled with tree_size, so the
// per-parent child counts matter more than the absolute row count.

const NUM_USERS = 50;
const NUM_ISSUES = 200;
const COMMENTS_PER_ISSUE = 15;
const LABELS_PER_ISSUE = 5;
const EMOJI_PER_ISSUE = 4;
const EMOJI_PER_COMMENT = 2;
const VIEWSTATE_PER_ISSUE = 3;
const NUM_LABELS = 20;

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

  for (let i = 0; i < NUM_USERS; i++) {
    add('user', {
      id: `user-${i}`,
      login: `user${i}`,
      name: `User ${i}`,
      avatar: `avatar${i}`,
      role: i % 10 === 0 ? 'crew' : 'user',
    });
  }

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

  for (let i = 0; i < NUM_LABELS; i++) {
    add('label', {
      id: `label-${i}`,
      name: `label-${i}`,
      projectID: `proj-${i % 2}`,
    });
  }

  const issues: Row[] = [];
  let commentSeq = 0;
  let emojiSeq = 0;
  for (let i = 0; i < NUM_ISSUES; i++) {
    const issueID = `issue-${i}`;
    const row: Row = {
      id: issueID,
      shortID: i,
      title: `Issue ${i}: Some bug or feature request`,
      open: i % 3 !== 0,
      modified: 1_700_000_000_000 - i * 1000,
      created: 1_700_000_000_000 - i * 2000,
      projectID: `proj-${i % 2}`,
      creatorID: `user-${i % NUM_USERS}`,
      assigneeID: `user-${(i + 1) % NUM_USERS}`,
      description: `Description for issue ${i}`,
      visibility: i % 5 === 0 ? 'internal' : 'public',
    };
    issues.push(row);
    add('issue', row);

    // Wide children: comments (each with its own creator + emoji children).
    for (let c = 0; c < COMMENTS_PER_ISSUE; c++) {
      const commentID = `comment-${commentSeq++}`;
      add('comment', {
        id: commentID,
        issueID,
        created: 1_700_000_000_000 - c * 500,
        body: `Comment ${c} on ${issueID}`,
        creatorID: `user-${c % NUM_USERS}`,
      });
      for (let e = 0; e < EMOJI_PER_COMMENT; e++) {
        add('emoji', {
          id: `emoji-${emojiSeq++}`,
          value: '👍',
          annotation: 'thumbsup',
          subjectID: commentID,
          creatorID: `user-${e % NUM_USERS}`,
          created: 1_700_000_000_000 - e * 100,
        });
      }
    }

    // Emoji directly on the issue.
    for (let e = 0; e < EMOJI_PER_ISSUE; e++) {
      add('emoji', {
        id: `emoji-${emojiSeq++}`,
        value: '🎉',
        annotation: 'tada',
        subjectID: issueID,
        creatorID: `user-${e % NUM_USERS}`,
        created: 1_700_000_000_000 - e * 100,
      });
    }

    // viewState rows.
    for (let v = 0; v < VIEWSTATE_PER_ISSUE; v++) {
      add('viewState', {
        issueID,
        userID: `user-${v % NUM_USERS}`,
        viewed: 1_700_000_000_000 - v * 10,
      });
    }

    // issueLabel join rows.
    for (let l = 0; l < LABELS_PER_ISSUE; l++) {
      add('issueLabel', {
        issueID,
        labelID: `label-${(i + l) % NUM_LABELS}`,
        projectID: `proj-${i % 2}`,
      });
    }
  }

  return {sources, issues};
}

const {sources, issues} = makeSources();

// The relationship-heavy query: wide (comments, emoji, labels, viewState) and
// deep (comment -> creator, comment -> emoji). This is the materialized shape
// that regressed.
const heavyQuery = builder.issue
  .related('creator')
  .related('assignee')
  .related('labels')
  .related('viewState')
  .related('emoji', e => e.related('creator'))
  .related('comments', c => c.related('creator').related('emoji'));

// ---- Hydration benchmarks ---------------------------------------------------
// Each iteration builds a fresh pipeline + ArrayView over the whole dataset and
// materializes it. This is the cold-load path that froze.

const hydrationOpts = {max_samples: 50};

describe('relationship hydration', () => {
  bench(
    'hydrate: issues only (baseline)',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(builder.issue);
    },
    hydrationOpts,
  );

  bench(
    'hydrate: issues + comments (wide, one level)',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(builder.issue.related('comments'));
    },
    hydrationOpts,
  );

  bench(
    'hydrate: issues + comments + emoji (wide, two levels)',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(
        builder.issue.related('comments', c => c.related('emoji')),
      );
    },
    hydrationOpts,
  );

  bench(
    'hydrate: heavy query (wide + deep) — regression case',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(heavyQuery);
    },
    hydrationOpts,
  );

  bench(
    'hydrate: heavy query limit(50)',
    async () => {
      const delegate = new QueryDelegateImpl({sources});
      await delegate.run(heavyQuery.limit(50));
    },
    hydrationOpts,
  );
});

// ---- Push benchmarks --------------------------------------------------------
// The view is materialized once in setup (not timed); the yielded fn pushes a
// single change per sample. With an immutable applyChange this is where a
// "new spine per push" would show up as allocation/GC cost — and where the
// reverted code re-walked the entire relationship tree on each push.

const pushOpts = {max_samples: 1_000};

function addAndTrack(source: MemorySource, row: Row, added: Row[]): void {
  added.push(row);
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

describe('push into relationship-heavy view', () => {
  let seq = 0;

  // Adding a top-level issue: with the reverted (eager) code this walked the
  // whole relationship tree of the new node. The new immutable code should only
  // path-copy the spine down to the inserted row.
  bench(
    'push: add issue into heavy view',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(heavyQuery);
      const added: Row[] = [];

      yield () => {
        addAndTrack(
          sources['issue'],
          {
            id: `push-issue-${seq++}`,
            shortID: NUM_ISSUES + seq,
            title: `Push Issue ${seq}`,
            open: true,
            modified: Date.now(),
            created: Date.now(),
            projectID: 'proj-0',
            creatorID: 'user-0',
            assigneeID: 'user-1',
            description: 'Pushed issue',
            visibility: 'public',
          },
          added,
        );
      };

      removeAll(sources['issue'], added);
      view.destroy();
    },
    pushOpts,
  );

  // Adding a child (comment) to an existing issue: a child change deep in the
  // tree. Should only touch the affected issue's subtree, not all issues.
  bench(
    'push: add comment (child) into heavy view',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(heavyQuery);
      const added: Row[] = [];

      yield () => {
        addAndTrack(
          sources['comment'],
          {
            id: `push-comment-${seq++}`,
            issueID: `issue-${seq % NUM_ISSUES}`,
            created: Date.now(),
            body: 'A new comment',
            creatorID: 'user-0',
          },
          added,
        );
      };

      removeAll(sources['comment'], added);
      view.destroy();
    },
    pushOpts,
  );

  // Editing a single top-level row's scalar field. The immutable path should
  // produce a new reference for exactly this row and keep every other row's
  // reference stable (the React.memo win).
  bench(
    'push: edit issue title in heavy view',
    function* () {
      const delegate = new QueryDelegateImpl({sources});
      const view = delegate.materialize(heavyQuery);
      let editCount = 0;
      // Track the current row at each index so each timed sample performs
      // exactly one push (the forward edit), matching the single-push-per-sample
      // shape of the other push benches. The edits are rolled back in the
      // cleanup section after the yield so the dataset stays stable for the next
      // bench.
      const current = issues.slice();

      yield () => {
        const idx = editCount % NUM_ISSUES;
        const oldRow = current[idx];
        const newRow = {...oldRow, title: `Edited ${editCount++}`};
        for (const _ of sources['issue'].push(
          makeSourceChangeEdit(newRow, oldRow as Row),
        )) {
          /* consume */
        }
        current[idx] = newRow;
      };

      // Restore any edited rows so the dataset is stable for the next bench.
      for (let i = 0; i < NUM_ISSUES; i++) {
        if (current[i] !== issues[i]) {
          for (const _ of sources['issue'].push(
            makeSourceChangeEdit(issues[i] as Row, current[i] as Row),
          )) {
            /* consume */
          }
        }
      }

      view.destroy();
    },
    pushOpts,
  );
});
