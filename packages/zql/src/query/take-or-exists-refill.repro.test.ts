/**
 * Regression test: a `limit` over an OR whose branches are an EXISTS and a
 * simple filter.
 *
 *   source -> UnionFanOut -> [exists | filter] -> UnionFanIn -> Take
 *
 * Removing the row the EXISTS branch matched on (the single `project`) evicts
 * every issue that only qualified through that branch. Take then has to refill
 * its window from the issues that still match via `x = 1`. What the view holds
 * after that push must be what a freshly materialized query over the same
 * final data holds.
 *
 * Run with the EXISTS both unflipped (a join under the fan-out) and flipped (a
 * FlippedJoin pushing from the child side), and both at the top level and
 * inside a `related` subquery, where the same pipeline is partitioned by the
 * parent row.
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Source} from '../ivm/source.ts';
import {makeSourceChangeAdd, makeSourceChangeRemove} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const ws = table('ws').columns({id: string()}).primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    wsID: string(),
    projectID: string(),
    x: number(),
  })
  .primaryKey('id');

const project = table('project').columns({id: string()}).primaryKey('id');

const schema = createSchema({
  tables: [ws, issue, project],
  relationships: [
    relationships(ws, ({many}) => ({
      issues: many({
        sourceField: ['id'],
        destField: ['wsID'],
        destSchema: issue,
      }),
    })),
    relationships(issue, ({many}) => ({
      project: many({
        sourceField: ['projectID'],
        destField: ['id'],
        destSchema: project,
      }),
    })),
  ],
});

const rows = {
  ws: [{id: 'w1'}],
  issue: [
    // i1 and i2 match only through the EXISTS branch...
    {id: 'i1', wsID: 'w1', projectID: 'p1', x: 0},
    {id: 'i2', wsID: 'w1', projectID: 'p1', x: 0},
    // ...i3 and i4 match through both branches, so they survive the removal
    // and are what the window refills with.
    {id: 'i3', wsID: 'w1', projectID: 'p1', x: 1},
    {id: 'i4', wsID: 'w1', projectID: 'p1', x: 1},
  ],
  project: [{id: 'p1'}],
} as const satisfies Record<string, readonly Row[]>;

function makeSources(withProject: boolean): Record<string, Source> {
  const sources: Record<string, Source> = {};
  for (const name of ['ws', 'issue', 'project'] as const) {
    const {columns, primaryKey} = schema.tables[name];
    const source = createSource(lc, testLogConfig, name, columns, primaryKey);
    if (name !== 'project' || withProject) {
      for (const row of rows[name]) {
        consume(source.push(makeSourceChangeAdd(row)));
      }
    }
    sources[name] = source;
  }
  return sources;
}

/** The two OR branches, over a `limit` small enough to make Take refill. */
const matchingIssues = (q: Query<'issue', typeof schema>, flip: boolean) =>
  q
    .where(({or, cmp, exists}) =>
      or(exists('project', undefined, {flip}), cmp('x', '=', 1)),
    )
    .orderBy('id', 'asc')
    .limit(2);

const topLevel = (flip: boolean) =>
  matchingIssues(newQuery(schema, 'issue'), flip);

const nested = (flip: boolean) =>
  newQuery(schema, 'ws').related('issues', q => matchingIssues(q, flip));

/**
 * Materializes `makeQuery()`, removes the project, and checks the view against
 * the same query materialized fresh over data that never had the project.
 */
function expectViewMatchesFreshQuery<
  TTable extends keyof (typeof schema)['tables'] & string,
  TReturn,
>(makeQuery: () => Query<TTable, typeof schema, TReturn>) {
  const sources = makeSources(true);
  const view = new QueryDelegateImpl({sources}).materialize(makeQuery());
  consume(must(sources.project).push(makeSourceChangeRemove({id: 'p1'})));

  const fresh = new QueryDelegateImpl({
    sources: makeSources(false),
  }).materialize(makeQuery());

  expect(view.data).toEqual(fresh.data);
}

test('top-level join', () => {
  expectViewMatchesFreshQuery(() => topLevel(false));
});

test('top-level flipped join', () => {
  expectViewMatchesFreshQuery(() => topLevel(true));
});

test('nested/partitioned join', () => {
  expectViewMatchesFreshQuery(() => nested(false));
});

test('nested/partitioned flipped join', () => {
  expectViewMatchesFreshQuery(() => nested(true));
});
