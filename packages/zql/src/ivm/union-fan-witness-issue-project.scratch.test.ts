// Scratch: the union-fan witness/refcount asymmetry on an issue/project schema.
// Backs the explainer artifact. Not for commit.
import {writeFileSync} from 'node:fs';
import {afterAll, expect, test} from 'vitest';
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
import {buildPipeline} from '../builder/builder.ts';
import {TestBuilderDelegate} from '../builder/test-builder-delegate.ts';
import {newQuery} from '../query/query-impl.ts';
import {asQueryInternals} from '../query/query-internals.ts';
import {Catch, type CaughtChange, type CaughtNode} from './catch.ts';
import type {Source, SourceChange} from './source.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
} from './source.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

const lc = createSilentLogContext();

const project = table('project')
  .columns({id: string(), active: boolean()})
  .primaryKey('id');
const issue = table('issue')
  .columns({
    id: number(),
    assignee: string().optional(),
    priority: string().optional(),
    projectID: string(),
  })
  .primaryKey('id');
const schema = createSchema({
  tables: [project, issue],
  relationships: [
    relationships(issue, ({one}) => ({
      project: one({
        sourceField: ['projectID'],
        destField: ['id'],
        destSchema: project,
      }),
    })),
  ],
});

// "Unassigned issues, plus every issue in an active project."
const q = newQuery(schema, 'issue').where(({or, cmp, exists}) =>
  or(
    cmp('assignee', 'IS', null),
    exists('project', p => p.where('active', true), {flip: true}),
  ),
);
let ast = asQueryInternals(q).ast;

// "Unassigned issues, plus high-priority issues in an active project."
const q4 = newQuery(schema, 'issue').where(({or, and, cmp, exists}) =>
  or(
    cmp('assignee', 'IS', null),
    and(
      cmp('priority', 'high'),
      exists('project', p => p.where('active', true), {flip: true}),
    ),
  ),
);

type Data = {project: Row[]; issue: Row[]};

function makeSources(data: Data): Record<string, Source> {
  const out: Record<string, Source> = {};
  for (const name of ['project', 'issue'] as const) {
    const {columns, primaryKey} = schema.tables[name];
    const s = createSource(lc, testLogConfig, name, columns, primaryKey);
    for (const row of data[name]) consume(s.push(makeSourceChangeAdd(row)));
    out[name] = s;
  }
  return out;
}

/** zero-cache's per-query row refcounts (Streamer + view-syncer). */
class Footprint {
  readonly rc = new Map<string, number>();
  #node(tbl: string, node: CaughtNode, delta: 1 | -1) {
    if (node === 'yield') return;
    const key = `${tbl}:${node.row.id}`;
    this.rc.set(key, (this.rc.get(key) ?? 0) + delta);
    for (const children of Object.values(node.relationships)) {
      for (const child of children) this.#node('project', child, delta);
    }
  }
  hydrate(nodes: CaughtNode[]) {
    for (const n of nodes) this.#node('issue', n, 1);
  }
  apply(c: CaughtChange, tbl = 'issue') {
    if (c.type === 'add') this.#node(tbl, c.node, 1);
    else if (c.type === 'remove') this.#node(tbl, c.node, -1);
    else if (c.type === 'child') this.apply(c.child.change, 'project');
  }
  rows(): string[] {
    return [...this.rc]
      .filter(([, n]) => n !== 0)
      .map(([k, n]) => `${k}=${n}`)
      .sort();
  }
}

const trace: string[] = [];
afterAll(() => {
  if (process.env.TRACE_OUT)
    writeFileSync(process.env.TRACE_OUT, trace.join('\n'));
});
function run(data: Data, pushes: [string, SourceChange][] = []) {
  const sources = makeSources(data);
  const c = new Catch(
    buildPipeline(ast, new TestBuilderDelegate(sources), 'q'),
  );
  const fp = new Footprint();
  const initial = c.fetch();
  fp.hydrate(initial);
  console.log('  HYDRATE', JSON.stringify(initial));
  trace.push('HYDRATE ' + JSON.stringify(initial));
  for (const [name, change] of pushes) {
    const before = c.pushes.length;
    consume(must(sources[name]).push(change));
    console.log('  PUSH', name, JSON.stringify(c.pushes.slice(before)));
    trace.push(name + ' ' + JSON.stringify(c.pushes.slice(before)));
  }
  for (const p of c.pushes) fp.apply(p);
  console.log('  REFCOUNTS', JSON.stringify([...fp.rc]));
  trace.push('REFCOUNTS ' + JSON.stringify([...fp.rc]));
  return fp.rows();
}

const p1 = {id: 'p1', active: true};
const i1 = {id: 1, assignee: null, projectID: 'p1'}; // passes BOTH branches
const i2 = {id: 2, assignee: 'ann', projectID: 'p1'}; // passes EXISTS only

test('1. over-decrement: deleting i1 takes p1 away from i2', () => {
  console.log('incremental');
  const inc = run({project: [p1], issue: [i1, i2]}, [
    ['issue', makeSourceChangeRemove(i1)],
  ]);
  console.log('fresh');
  const fresh = run({project: [p1], issue: [i2]});
  expect(inc).toEqual(fresh);
});

test('2. leak: archiving p1 never reaches the client', () => {
  console.log('incremental');
  const inc = run({project: [p1], issue: []}, [
    ['issue', makeSourceChangeAdd(i1)],
    ['project', makeSourceChangeEdit({id: 'p1', active: false}, p1)],
  ]);
  console.log('fresh');
  const fresh = run({project: [{id: 'p1', active: false}], issue: [i1]});
  expect(inc).toEqual(fresh);
});

test('3. edit: assigning i1 leaves it with no witness', () => {
  console.log('incremental');
  const inc = run({project: [p1], issue: [i1]}, [
    ['issue', makeSourceChangeEdit({...i1, assignee: 'ann'}, i1)],
  ]);
  console.log('fresh');
  const fresh = run({project: [p1], issue: [{...i1, assignee: 'ann'}]});
  expect(inc).toEqual(fresh);
});

test('4. edit into a flipped branch while another branch holds the row', () => {
  const saved = ast;
  ast = asQueryInternals(q4).ast;
  try {
    const low = {id: 1, assignee: null, priority: 'low', projectID: 'p1'};
    const high = {...low, priority: 'high'};
    const assigned = {...high, assignee: 'ann'};
    console.log('incremental');
    const inc = run({project: [p1], issue: [low]}, [
      ['issue', makeSourceChangeEdit(high, low)],
      ['issue', makeSourceChangeEdit(assigned, high)],
    ]);
    console.log('fresh');
    const fresh = run({project: [p1], issue: [assigned]});
    expect(inc).toEqual(fresh);
  } finally {
    ast = saved;
  }
});
