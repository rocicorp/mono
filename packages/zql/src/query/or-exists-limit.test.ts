// or(filter, exists) under a limit: removing the row a join key points at
// drops several issues at once, some inside the Take's bound and some past
// it, while other issues sharing the same ord survive through the filter
// branch. The view must end up where a fresh materialization does.
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {number, table} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Source} from '../ivm/source.ts';
import {makeSourceChangeAdd, makeSourceChangeRemove} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const project = table('project').columns({id: number()}).primaryKey('id');
const issue = table('issue')
  .columns({id: number(), projectID: number(), x: number(), ord: number()})
  .primaryKey('id');

const schema = createSchema({
  tables: [project, issue],
  relationships: [
    relationships(issue, ({many}) => ({
      project: many({
        sourceField: ['projectID'],
        destField: ['id'],
        destSchema: project,
      }),
    })),
  ],
});

type I = [id: number, projectID: number, x: number, ord: number];

function makeSources(projects: number[], issues: I[]): Record<string, Source> {
  const rows: Record<string, Row[]> = {
    project: projects.map(id => ({id})),
    issue: issues.map(([id, projectID, x, ord]) => ({id, projectID, x, ord})),
  };
  const sources: Record<string, Source> = {};
  for (const name of ['project', 'issue'] as const) {
    const {columns, primaryKey} = schema.tables[name];
    const source = createSource(lc, testLogConfig, name, columns, primaryKey);
    for (const row of rows[name]) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  return sources;
}

function check(issues: I[], want: number[], flip: boolean) {
  const q = () =>
    newQuery(schema, 'issue')
      .where(({or, cmp, exists}) =>
        or(
          cmp('x', '=', 1),
          exists('project', p => p, {flip}),
        ),
      )
      .orderBy('ord', 'asc')
      .orderBy('id', 'asc')
      .limit(2);
  const sources = makeSources([1, 2], issues);
  const view = new QueryDelegateImpl({sources}).materialize(q());
  consume(must(sources.project).push(makeSourceChangeRemove({id: 1})));
  const fresh = new QueryDelegateImpl({
    sources: makeSources([2], issues),
  }).materialize(q());
  const ids = (d: unknown) => (d as {id: number}[]).map(r => r.id);
  expect(ids(fresh.data)).toEqual(want);
  expect(ids(view.data)).toEqual(want);
}

//   ord:      10   20   30   40   50   60   70   80
//   issue:     5    1    3    8    4    2    7    6
//   project:  p1   p1   p1   p1   p2   p2   p2   p1
//   x = 1:    yes                 yes       yes  yes
const interleaved: I[] = [
  [5, 1, 1, 10],
  [1, 1, 0, 20],
  [3, 1, 0, 30],
  [8, 1, 0, 40],
  [4, 2, 1, 50],
  [2, 2, 0, 60],
  [7, 2, 1, 70],
  [6, 1, 1, 80],
];

//   ord:      10   20   30   40   50
//   issue:     1    2    3    4    9
//   project:  p1   p1   p1   p1   p2
//   x = 1:                        yes
const boundStillPending: I[] = [
  [1, 1, 0, 10],
  [2, 1, 0, 20],
  [3, 1, 0, 30],
  [4, 1, 0, 40],
  [9, 2, 1, 50],
];

test('interleaved survivors, join', () => check(interleaved, [5, 4], false));
test('interleaved survivors, flipped join', () =>
  check(interleaved, [5, 4], true));
test('bound still pending, join', () => check(boundStillPending, [9], false));
test('bound still pending, flipped join', () =>
  check(boundStillPending, [9], true));
