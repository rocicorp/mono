// Scratch port of Rindle's follow-ups/union-fan-normalized-wire.md
// (`Employee(rel:Employee,ex:Employee)|flip[1]`, walk step 1). Not for commit.
//
// Folds the pipeline's change stream the way zero-cache does:
// pipeline-driver.ts `Streamer.#streamNodes` walks EVERY relationship on an
// add/remove node, and the CVR does refCounts[queryID]++ / -- per row
// (view-syncer.ts), deleting the row from the client when no count is > 0.
import {expect, test} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import {buildPipeline} from '../builder/builder.ts';
import {TestBuilderDelegate} from '../builder/test-builder-delegate.ts';
import {Catch, type CaughtChange, type CaughtNode} from './catch.ts';
import type {Source} from './source.ts';
import {makeSourceChangeAdd, makeSourceChangeRemove} from './source.ts';
import {consume} from './stream.ts';
import {createSource} from './test/source-factory.ts';

const lc = createSilentLogContext();

const columns = {
  id: {type: 'number'},
  name: {type: 'string'},
  reportsTo: {type: 'number', optional: true},
} as const;

function employees(rows: Row[]): Record<string, Source> {
  const source = createSource(lc, testLogConfig, 'employee', columns, ['id']);
  for (const row of rows) {
    consume(source.push(makeSourceChangeAdd(row)));
  }
  return {employee: source};
}

// employee WHERE EXISTS(reports, flip) OR reportsTo IS NULL
//   .related(manager) ORDER BY reportsTo, id LIMIT 2
const ast: AST = {
  table: 'employee',
  orderBy: [
    ['reportsTo', 'asc'],
    ['id', 'asc'],
  ],
  limit: 2,
  where: {
    type: 'or',
    conditions: [
      {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        flip: true,
        related: {
          system: 'client',
          correlation: {parentField: ['id'], childField: ['reportsTo']},
          subquery: {
            table: 'employee',
            alias: 'reports',
            orderBy: [['id', 'asc']],
          },
        },
      },
      {
        type: 'simple',
        op: 'IS',
        left: {type: 'column', name: 'reportsTo'},
        right: {type: 'literal', value: null},
      },
    ],
  },
  related: [
    {
      system: 'client',
      correlation: {parentField: ['reportsTo'], childField: ['id']},
      subquery: {
        table: 'employee',
        alias: 'manager',
        orderBy: [['id', 'asc']],
      },
    },
  ],
};

/** The CVR's per-query row refcounts, fed the way `Streamer` feeds them. */
class Footprint {
  readonly rc = new Map<number, number>();

  #node(node: CaughtNode, delta: 1 | -1) {
    if (node === 'yield') return;
    const id = node.row.id as number;
    this.rc.set(id, (this.rc.get(id) ?? 0) + delta);
    for (const children of Object.values(node.relationships)) {
      for (const child of children) this.#node(child, delta);
    }
  }

  hydrate(nodes: CaughtNode[]) {
    for (const n of nodes) this.#node(n, 1);
  }

  apply(change: CaughtChange): void {
    switch (change.type) {
      case 'add':
        return this.#node(change.node, 1);
      case 'remove':
        return this.#node(change.node, -1);
      case 'child':
        return this.apply(change.child.change);
      case 'edit':
        return; // row only, no subtree
    }
  }

  /** Rows the client holds: every row with a positive count. */
  ids(): number[] {
    return [...this.rc]
      .filter(([, n]) => n > 0)
      .map(([id]) => id)
      .sort();
  }
}

const e1 = {id: 1, name: 'Adams', reportsTo: null};
const e2 = {id: 2, name: 'Mills', reportsTo: 1};
const e3 = {id: 3, name: 'Park', reportsTo: 2};

function run(rows: Row[]) {
  const sources = employees(rows);
  const delegate = new TestBuilderDelegate(sources);
  const c = new Catch(buildPipeline(ast, delegate, 'q'));
  const fp = new Footprint();
  const initial = c.fetch();
  fp.hydrate(initial);
  return {sources, c, fp, initial};
}

test('union fan: hydrate vs push disagree on the witness subtree', () => {
  const {sources, c, fp, initial} = run([e1, e2, e3]);

  // E1 passes BOTH branches (reportsTo IS NULL, and E2 reports to it), but
  // `mergeFetches` keeps the first (filter) branch's node: no `reports`.
  console.log('HYDRATE', JSON.stringify(initial));

  consume(must(sources.employee).push(makeSourceChangeRemove(e1)));
  console.log('PUSHES', JSON.stringify(c.pushes));
  for (const p of c.pushes) fp.apply(p);

  // What a fresh hydrate over the post-push data syncs.
  const fresh = run([e2, e3]);
  console.log('incremental footprint', fp.ids(), 'fresh', fresh.fp.ids());
  console.log('raw refcounts', JSON.stringify([...fp.rc]));
  expect(fp.ids()).toEqual(fresh.fp.ids());
});

// The other direction (Rindle's `Track(ex:MediaType,ex:Track)|flip[01]`, step 9):
// a row added through the fan-out PUSH path carries the merged witness subtree,
// but when its last witness is deleted the FlippedJoin's Remove is an *internal*
// change, and `#pushInternalChange` drops it because another branch still holds
// the row. The witness is never decremented: the client keeps a row that no
// longer exists upstream.
test('union fan: a dropped internal remove leaks the witness', () => {
  const e4 = {id: 4, name: 'Witness', reportsTo: 1};
  const {sources, c, fp, initial} = run([e4]);
  console.log('HYDRATE', JSON.stringify(initial));

  consume(must(sources.employee).push(makeSourceChangeAdd(e1)));
  consume(must(sources.employee).push(makeSourceChangeRemove(e4)));
  console.log('PUSHES', JSON.stringify(c.pushes));
  for (const p of c.pushes) fp.apply(p);

  const fresh = run([e1]);
  console.log('incremental footprint', fp.ids(), 'fresh', fresh.fp.ids());
  expect(fp.ids()).toEqual(fresh.fp.ids());
});
