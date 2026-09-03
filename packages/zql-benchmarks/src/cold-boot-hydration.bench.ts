/**
 * Cold-boot hydration: N queries are materialized before the replica has been
 * loaded into the IVM sources (what happens on the client when `useQuery`
 * runs before `ZeroRep.init` completes).
 *
 * - "live pipelines": every row of the replica is pushed through every
 *   already-connected pipeline.
 * - "deferred pipelines": pipelines are built after the load and each view is
 *   hydrated once, via `QueryDelegate.pipelinesReady` / `onPipelinesReady`.
 *
 * Run with:
 *   pnpm --filter zql-benchmarks run bench cold-boot-hydration
 */

import {bench, describe} from '../../shared/src/bench.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {makeSourceChangeAdd} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import type {TypedView} from '../../zql/src/query/typed-view.ts';
import {builder, schema} from './schema.ts';

const NUM_USERS = 200;
const NUM_ISSUES = 5_000;
const NUM_COMMENTS = 15_000;
const NUM_LABELS = 50;
const NUM_ISSUE_LABELS = 10_000;

function makeSources(): Record<string, MemorySource> {
  const sources: Record<string, MemorySource> = {};
  for (const [name, tableSchema] of Object.entries(schema.tables)) {
    sources[name] = new MemorySource(
      tableSchema.name,
      tableSchema.columns,
      tableSchema.primaryKey,
    );
  }
  return sources;
}

function* rows(): Generator<[string, Row]> {
  for (let i = 0; i < NUM_USERS; i++) {
    yield [
      'user',
      {
        id: `user-${i}`,
        login: `user${i}`,
        name: `User ${i}`,
        avatar: `avatar${i}`,
        role: i % 10 === 0 ? 'crew' : 'user',
      },
    ];
  }
  for (let i = 0; i < 5; i++) {
    yield [
      'project',
      {id: `proj-${i}`, name: `Project ${i}`, lowerCaseName: `project ${i}`},
    ];
  }
  for (let i = 0; i < NUM_ISSUES; i++) {
    yield [
      'issue',
      {
        id: `issue-${String(i).padStart(6, '0')}`,
        shortID: i,
        title: `Issue ${i}: ${i % 7 === 0 ? 'bug' : 'feature'} request`,
        open: i % 3 !== 0,
        modified: 1_700_000_000_000 - i * 1000,
        created: 1_700_000_000_000 - i * 2000,
        projectID: `proj-${i % 5}`,
        creatorID: `user-${i % NUM_USERS}`,
        assigneeID: i % 4 === 0 ? undefined : `user-${(i + 1) % NUM_USERS}`,
        description: `Description for issue ${i}`,
        visibility: i % 5 === 0 ? 'internal' : 'public',
      },
    ];
  }
  for (let i = 0; i < NUM_COMMENTS; i++) {
    yield [
      'comment',
      {
        id: `comment-${String(i).padStart(6, '0')}`,
        issueID: `issue-${String(i % NUM_ISSUES).padStart(6, '0')}`,
        created: 1_700_000_000_000 - i * 500,
        body: `Comment body ${i}`,
        creatorID: `user-${i % NUM_USERS}`,
      },
    ];
  }
  for (let i = 0; i < NUM_LABELS; i++) {
    yield [
      'label',
      {id: `label-${i}`, name: `label-${i}`, projectID: `proj-${i % 5}`},
    ];
  }
  for (let n = 0; n < NUM_ISSUE_LABELS; n++) {
    yield [
      'issueLabel',
      {
        issueID: `issue-${String(n % NUM_ISSUES).padStart(6, '0')}`,
        labelID: `label-${(n * 13 + Math.floor(n / NUM_ISSUES)) % NUM_LABELS}`,
        projectID: `proj-${n % 5}`,
      },
    ];
  }
}

function load(sources: Record<string, MemorySource>) {
  for (const [table, row] of rows()) {
    consume(sources[table].push(makeSourceChangeAdd(row)));
  }
}

// A representative set of queries an app registers at startup.
const QUERIES: (() => AnyQuery)[] = [
  () =>
    builder.issue
      .orderBy('modified', 'desc')
      .limit(100)
      .related('creator')
      .related('assignee')
      .related('labels'),
  () =>
    builder.issue
      .where('open', true)
      .orderBy('modified', 'desc')
      .limit(100)
      .related('creator')
      .related('labels'),
  () =>
    builder.issue
      .where('projectID', 'proj-0')
      .limit(100)
      .related('labels')
      .related('comments', q => q.limit(10).related('creator')),
  () => builder.issue.where('creatorID', 'user-1').related('project'),
  () => builder.user,
  () => builder.label,
  () => builder.comment.where('issueID', 'issue-000001').related('creator'),
  () =>
    builder.issue
      .where('id', 'issue-000005')
      .related('comments', q => q.related('creator'))
      .related('labels')
      .related('creator'),
  () => builder.issue.orderBy('created', 'asc').limit(50),
  () => builder.issue.orderBy('title', 'asc').limit(50),
  () => builder.comment.orderBy('created', 'desc').limit(200).related('issue'),
  () => builder.issue.whereExists('comments').limit(100),
  () => builder.issue.related('comments', q => q.limit(1)).limit(100),
];

class DeferredDelegate extends QueryDelegateImpl {
  #ready = false;
  readonly #pending = new Set<() => void>();

  override get pipelinesReady(): boolean {
    return this.#ready;
  }

  override onPipelinesReady(cb: () => void): () => void {
    this.#pending.add(cb);
    return () => {
      this.#pending.delete(cb);
    };
  }

  markReady(): void {
    this.#ready = true;
    const pending = [...this.#pending];
    this.#pending.clear();
    this.batchViewUpdates(() => {
      for (const attach of pending) {
        attach();
      }
    });
    this.commit();
  }
}

const opts = {max_samples: 20};

describe('cold boot hydration', () => {
  bench(
    'materialize then load: live pipelines',
    () => {
      const sources = makeSources();
      const delegate = new QueryDelegateImpl({sources});
      const views: TypedView<unknown>[] = QUERIES.map(q =>
        delegate.materialize(q()),
      );
      load(sources);
      delegate.commit();
      for (const v of views) {
        v.destroy();
      }
    },
    opts,
  );

  bench(
    'materialize then load: deferred pipelines',
    () => {
      const sources = makeSources();
      const delegate = new DeferredDelegate({sources});
      const views: TypedView<unknown>[] = QUERIES.map(q =>
        delegate.materialize(q()),
      );
      load(sources);
      delegate.markReady();
      for (const v of views) {
        v.destroy();
      }
    },
    opts,
  );

  bench(
    'load then materialize (lower bound)',
    () => {
      const sources = makeSources();
      load(sources);
      const delegate = new QueryDelegateImpl({sources});
      const views: TypedView<unknown>[] = QUERIES.map(q =>
        delegate.materialize(q()),
      );
      for (const v of views) {
        v.destroy();
      }
    },
    opts,
  );
});
