/**
 * Regression test: an add that reaches `Take` while a deferred refill is
 * pending.
 *
 * `Take` no longer refills its window when an in-window row is removed. It
 * records a deficit and refills in `reconcile`, which the source runs once
 * after it has pushed the change to every connection. Between those two
 * points `size < limit`, and `Take` accepts any add in that state -- an
 * assumption that only held while `size < limit` meant the input was
 * exhausted. An add from a later connection of the same source change is
 * therefore admitted even when rows that sort before it should refill the
 * window first.
 *
 * One `project` edit reaches the query through two connections, one per
 * relationship: it drops i1 (matched via `project.flag = true`) and adds i9
 * (matched via `otherProject.flag = false`). i3 still matches through
 * `x = 1`, so the window should become [i2, i3]; the view ends up [i2, i9].
 *
 * Whether this happens depends on which connection pushes first, so each shape
 * runs with the two relationships' roles swapped.
 */
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import type {Source} from '../ivm/source.ts';
import {makeSourceChangeAdd, makeSourceChangeEdit} from '../ivm/source.ts';
import {consume} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const project = table('project')
  .columns({id: string(), flag: boolean()})
  .primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    projectID: string(),
    otherProjectID: string(),
    x: number(),
  })
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
      otherProject: many({
        sourceField: ['otherProjectID'],
        destField: ['id'],
        destSchema: project,
      }),
    })),
  ],
});

type Relationship = 'project' | 'otherProject';

const before = {id: 'p', flag: true};
const after = {id: 'p', flag: false};

/**
 * `loses` is the relationship through which the edit removes i1, `gains` the
 * one through which it adds i9.
 */
function issueRows(loses: Relationship, gains: Relationship): Row[] {
  const row = (id: string, via: Relationship | undefined, x: number) => ({
    id,
    projectID: via === 'project' ? 'p' : 'none',
    otherProjectID: via === 'otherProject' ? 'p' : 'none',
    x,
  });
  return [
    row('i1', loses, 0), // in the window, leaves it
    row('i2', undefined, 1), // in the window, stays
    row('i3', undefined, 1), // should refill the window
    row('i9', gains, 0), // joins the result past i3
  ];
}

function makeSources(projectRow: Row, issues: Row[]): Record<string, Source> {
  const sources: Record<string, Source> = {};
  for (const [name, rows] of [
    ['project', [projectRow]],
    ['issue', issues],
  ] as const) {
    const {columns, primaryKey} = schema.tables[name];
    const source = createSource(lc, testLogConfig, name, columns, primaryKey);
    for (const row of rows) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = source;
  }
  return sources;
}

function expectViewMatchesFreshQuery(
  flip: boolean,
  loses: Relationship,
  gains: Relationship,
) {
  const makeQuery = () =>
    newQuery(schema, 'issue')
      .where(({or, cmp, exists}) =>
        or(
          cmp('x', '=', 1),
          exists(loses, p => p.where('flag', '=', true), {flip}),
          exists(gains, p => p.where('flag', '=', false), {flip}),
        ),
      )
      .orderBy('id', 'asc')
      .limit(2);

  const issues = issueRows(loses, gains);
  const sources = makeSources(before, issues);
  const view = new QueryDelegateImpl({sources}).materialize(makeQuery());
  consume(must(sources.project).push(makeSourceChangeEdit(after, before)));

  const fresh = new QueryDelegateImpl({
    sources: makeSources(after, issues),
  }).materialize(makeQuery());

  expect(view.data).toEqual(fresh.data);
}

test('join: loses via project, gains via otherProject', () => {
  expectViewMatchesFreshQuery(false, 'project', 'otherProject');
});

test('join: loses via otherProject, gains via project', () => {
  expectViewMatchesFreshQuery(false, 'otherProject', 'project');
});

test('flipped join: loses via project, gains via otherProject', () => {
  expectViewMatchesFreshQuery(true, 'project', 'otherProject');
});

test('flipped join: loses via otherProject, gains via project', () => {
  expectViewMatchesFreshQuery(true, 'otherProject', 'project');
});
