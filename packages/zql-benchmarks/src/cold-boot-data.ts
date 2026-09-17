/**
 * Data set and startup query set shared by the cold-boot benchmarks.
 */
import type {Row} from '../../zero-protocol/src/data.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {makeSourceChangeAdd} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {builder, schema} from './schema.ts';

const NUM_USERS = 200;
const NUM_LABELS = 50;

export function makeSources(): Record<string, MemorySource> {
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

export function* rows(scale: number): Generator<[string, Row]> {
  const NUM_ISSUES = 5_000 * scale;
  const NUM_COMMENTS = 15_000 * scale;
  const NUM_ISSUE_LABELS = 10_000 * scale;
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

export function load(sources: Record<string, MemorySource>, scale = 1) {
  for (const [table, row] of rows(scale)) {
    consume(sources[table].push(makeSourceChangeAdd(row)));
  }
}

/**
 * Bulk variant of {@link load}, as `IVMSourceBranch.advance` does for an empty
 * source: a table's rows go in with one `MemorySource.pushAdds` instead of one
 * `push` per row.
 */
export function loadBulk(scale = 1): Record<string, MemorySource> {
  const byTable = new Map<string, Row[]>();
  for (const [table, row] of rows(scale)) {
    let a = byTable.get(table);
    if (!a) {
      byTable.set(table, (a = []));
    }
    a.push(row);
  }
  const sources = makeSources();
  for (const [table, tableRows] of byTable) {
    sources[table].pushAdds(tableRows);
  }
  return sources;
}

// A representative set of queries an app registers at startup.
export const QUERIES: (() => AnyQuery)[] = [
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

// Larger, mostly unlimited list queries in the style of an app that keeps a
// whole "library" in views. With QUERIES this makes a 21 query startup burst.
export const LIBRARY_QUERIES: (() => AnyQuery)[] = [
  () => builder.issue.where('projectID', 'proj-1').related('creator'),
  () => builder.issue.where('projectID', 'proj-2').related('labels'),
  () =>
    builder.issue
      .where('open', true)
      .orderBy('created', 'desc')
      .limit(2_000)
      .related('creator')
      .related('assignee'),
  () => builder.comment.where('creatorID', 'user-3').related('issue'),
  () => builder.comment.orderBy('created', 'asc').limit(2_000),
  () => builder.issue.where('visibility', 'internal').orderBy('shortID', 'asc'),
  () =>
    builder.issue
      .where('assigneeID', 'user-7')
      .related('comments', q => q.orderBy('created', 'desc').limit(5)),
  () => builder.issueLabel.where('labelID', 'label-3').related('issue'),
];
