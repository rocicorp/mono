/**
 * Regression test: a child change that fans out over an unordered parent
 * stream.
 *
 * EXISTS children connect to their source without a sort, so SQLite returns
 * their rows in whatever order its plan visits (e.g. rowid order, which a
 * delete + re-insert changes). While `Join` pushes a child change to each
 * matching parent in turn, it overlays the change onto the relationship of
 * every parent it has not reached yet, so fetches see those parents as they
 * were. It used to decide "not reached yet" by comparing the parent to the
 * current one with `compareRows`, which only holds when the stream is in
 * `compareRows` order.
 *
 * Here removing invoice i1 reaches line l2 before line l1 (the source below
 * reverses unordered fetches, as SQLite does after l1 is re-inserted). With
 * the old check, l1 counted as already pushed, so while `Take` handled t2 it
 * saw t1 as already gone:
 * - `whereExists ... desc`: `Take` removed its bound t2, found no row before
 *   it, and stored `bound: undefined` with size 1, then dropped the removal of
 *   t1 as "after the bound". The view kept t1.
 * - `NOT EXISTS ... asc`: the add of tA made `Take` adopt tC (not yet added)
 *   as its bound, so re-adding the invoice removed tC from a view that did not
 *   hold it (`node does not exist`).
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
import type {Node} from '../ivm/data.ts';
import type {Source} from '../ivm/source.ts';
import {makeSourceChangeAdd, makeSourceChangeRemove} from '../ivm/source.ts';
import {consume, type Stream} from '../ivm/stream.ts';
import {createSource} from '../ivm/test/source-factory.ts';
import {newQuery} from './query-impl.ts';
import {QueryDelegateImpl} from './test/query-delegate.ts';

const lc = createSilentLogContext();

const track = table('track')
  .columns({id: string(), ms: number()})
  .primaryKey('id');

const line = table('line')
  .columns({id: string(), trackID: string(), invoiceID: string()})
  .primaryKey('id');

const invoice = table('invoice').columns({id: string()}).primaryKey('id');

const schema = createSchema({
  tables: [track, line, invoice],
  relationships: [
    relationships(track, ({many}) => ({
      lines: many({
        sourceField: ['id'],
        destField: ['trackID'],
        destSchema: line,
      }),
    })),
    relationships(line, ({one}) => ({
      invoice: one({
        sourceField: ['invoiceID'],
        destField: ['id'],
        destSchema: invoice,
      }),
    })),
  ],
});

/** Returns unordered fetches in reverse primary key order. */
function reverseUnorderedFetches(source: Source): Source {
  return {
    get tableSchema() {
      return source.tableSchema;
    },
    connect(sort, filters, splitEditKeys, debug) {
      const input = source.connect(sort, filters, splitEditKeys, debug);
      if (sort !== undefined) {
        return input;
      }
      return {
        fullyAppliedFilters: input.fullyAppliedFilters,
        getSchema: () => input.getSchema(),
        setOutput: output => input.setOutput(output),
        destroy: () => input.destroy(),
        *fetch(req): Stream<Node | 'yield'> {
          const nodes: Node[] = [];
          for (const node of input.fetch(req)) {
            if (node !== 'yield') {
              nodes.push(node);
            }
          }
          yield* nodes.reverse();
        },
      };
    },
    push: change => source.push(change),
    genPush: change => source.genPush(change),
  };
}

function makeSources(data: Record<string, Row[]>): Record<string, Source> {
  const sources: Record<string, Source> = {};
  for (const [name, rows] of Object.entries(data)) {
    const {columns, primaryKey} =
      schema.tables[name as keyof typeof schema.tables];
    const source = createSource(lc, testLogConfig, name, columns, primaryKey);
    for (const row of rows) {
      consume(source.push(makeSourceChangeAdd(row)));
    }
    sources[name] = name === 'line' ? reverseUnorderedFetches(source) : source;
  }
  return sources;
}

type Step = {table: string; kind: 'add' | 'remove'; row: Row};

function expectViewMatchesFreshQueries(
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  makeQuery: () => any,
  data: Record<string, Row[]>,
  steps: Step[],
) {
  const sources = makeSources(data);
  const view = new QueryDelegateImpl({sources}).materialize(makeQuery());
  const current = Object.fromEntries(
    Object.entries(data).map(([name, rows]) => [name, [...rows]]),
  );
  for (const {table, kind, row} of steps) {
    const source = must(sources[table]);
    if (kind === 'remove') {
      consume(source.push(makeSourceChangeRemove(row)));
      current[table] = current[table].filter(r => r.id !== row.id);
    } else {
      consume(source.push(makeSourceChangeAdd(row)));
      current[table].push(row);
    }
    const fresh = new QueryDelegateImpl({
      sources: makeSources(current),
    }).materialize(makeQuery());
    expect(view.data).toEqual(fresh.data);
  }
}

const i1 = {id: 'i1'};
const i2 = {id: 'i2'};

test('EXISTS desc: removing the bound first keeps the rest of the window', () => {
  expectViewMatchesFreshQueries(
    () =>
      newQuery(schema, 'track')
        .whereExists('lines', l => l.whereExists('invoice'))
        .orderBy('ms', 'desc')
        .limit(2),
    {
      track: [
        {id: 't1', ms: 300},
        {id: 't2', ms: 200}, // the bound
        {id: 't3', ms: 100}, // refills the window
      ],
      // Reversed, i1's lines are reached as l2 (t2) then l1 (t1).
      line: [
        {id: 'l1', trackID: 't1', invoiceID: 'i1'},
        {id: 'l2', trackID: 't2', invoiceID: 'i1'},
        {id: 'l3', trackID: 't3', invoiceID: 'i2'},
      ],
      invoice: [i1, i2],
    },
    [{table: 'invoice', kind: 'remove', row: i1}],
  );
});

test('NOT EXISTS asc: an add does not adopt a row not yet pushed as bound', () => {
  expectViewMatchesFreshQueries(
    () =>
      newQuery(schema, 'track')
        .where(({exists, not}) =>
          not(exists('lines', l => l.whereExists('invoice'))),
        )
        .orderBy('ms', 'asc')
        .limit(2),
    {
      track: [
        {id: 'tA', ms: 100},
        {id: 'tB', ms: 150},
        {id: 'tC', ms: 200},
        {id: 'tD', ms: 250},
      ],
      // Reversed, i1's lines are reached as l2 (tA) then l1 (tC).
      line: [
        {id: 'l1', trackID: 'tC', invoiceID: 'i1'},
        {id: 'l2', trackID: 'tA', invoiceID: 'i1'},
      ],
      invoice: [i1],
    },
    [
      {table: 'invoice', kind: 'remove', row: i1},
      {table: 'invoice', kind: 'add', row: i1},
    ],
  );
});
