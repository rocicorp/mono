/**
 * Oracle / differential test for the aggregate **push** path.
 *
 * From a review comment (Matt): the push path is the fickle part, so instead of
 * hand-asserting expected values, drive a *sequence* of mutations through the
 * IVM aggregate operator and, after **each** push, assert the materialized value
 * equals what z2s computes in Postgres for the same data state. Postgres (via
 * z2s) is the oracle; the IVM operator is the unit under test.
 *
 * One `Query` drives both sides — it is materialized for the IVM result and its
 * AST is compiled by z2s for the oracle result — so the two can never drift in
 * *what* they compute, only in the answer (which is the bug we're hunting).
 *
 * Covered: top-level and relationship aggregates for all five functions, plus
 * junction (many-to-many) aggregates, over sequences that exercise the tricky
 * cases — min/max boundary removal (the non-invertible re-fetch), null↔value
 * transitions, a child moving between groups, a group emptying, and (for
 * junctions) a destination edit arriving as a CHILD change.
 */
import type {JSONValue} from 'postgres';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import {
  makeSourceChangeAdd,
  makeSourceChangeEdit,
  makeSourceChangeRemove,
  type Source,
} from '../../zql/src/ivm/source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {createSource} from '../../zql/src/ivm/test/source-factory.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {QueryDelegateImpl} from '../../zql/src/query/test/query-delegate.ts';
import {compile, extractZqlResult} from './compiler.ts';
import {formatPgInternalConvert} from './sql.ts';

const lc = createSilentLogContext();

// ---- schema (shared by IVM sources, z2s compile, and the Postgres DDL) ----

const issue = table('issue')
  .columns({id: string(), title: string()})
  .primaryKey('id');
const comment = table('comment')
  .columns({id: string(), issueId: string(), points: number().optional()})
  .primaryKey('id');
const label = table('label')
  .columns({id: string(), weight: number().optional()})
  .primaryKey('id');
const issueLabel = table('issueLabel')
  .columns({issueId: string(), labelId: string()})
  .primaryKey('issueId', 'labelId');

const issueRelationships = relationships(issue, ({many}) => ({
  comments: many({
    sourceField: ['id'],
    destField: ['issueId'],
    destSchema: comment,
  }),
  labels: many(
    {sourceField: ['id'], destField: ['issueId'], destSchema: issueLabel},
    {sourceField: ['labelId'], destField: ['id'], destSchema: label},
  ),
}));

const schema = createSchema({
  tables: [issue, comment, label, issueLabel],
  relationships: [issueRelationships],
});

const serverSchema: ServerSchema = {
  issue: {
    id: {type: 'text', isArray: false, isEnum: false},
    title: {type: 'text', isArray: false, isEnum: false},
  },
  comment: {
    id: {type: 'text', isArray: false, isEnum: false},
    issueId: {type: 'text', isArray: false, isEnum: false},
    points: {type: 'numeric', isArray: false, isEnum: false},
  },
  label: {
    id: {type: 'text', isArray: false, isEnum: false},
    weight: {type: 'numeric', isArray: false, isEnum: false},
  },
  issueLabel: {
    issueId: {type: 'text', isArray: false, isEnum: false},
    labelId: {type: 'text', isArray: false, isEnum: false},
  },
};

const DDL = `
  CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT NOT NULL);
  CREATE TABLE comment (id TEXT PRIMARY KEY, "issueId" TEXT NOT NULL, points INT);
  CREATE TABLE label (id TEXT PRIMARY KEY, weight INT);
  CREATE TABLE "issueLabel" (
    "issueId" TEXT NOT NULL, "labelId" TEXT NOT NULL,
    PRIMARY KEY ("issueId", "labelId"));
`;

const PK: Record<string, string[]> = {
  issue: ['id'],
  comment: ['id'],
  label: ['id'],
  issueLabel: ['issueId', 'labelId'],
};

const ident = (s: string) => `"${s}"`;

// ---- mutations applied to both the IVM source and Postgres ----

type Op =
  | {kind: 'insert'; table: string; row: Row}
  | {kind: 'remove'; table: string; row: Row}
  | {kind: 'edit'; table: string; old: Row; new: Row};

function applyIvm(sources: Record<string, Source>, op: Op): void {
  const src = sources[op.table];
  switch (op.kind) {
    case 'insert':
      consume(src.push(makeSourceChangeAdd(op.row)));
      break;
    case 'remove':
      consume(src.push(makeSourceChangeRemove(op.row)));
      break;
    case 'edit':
      consume(src.push(makeSourceChangeEdit(op.new, op.old)));
      break;
  }
}

async function applyPg(pg: PostgresDB, op: Op): Promise<void> {
  const pk = PK[op.table];
  switch (op.kind) {
    case 'insert': {
      const cols = Object.keys(op.row);
      await pg.unsafe(
        `INSERT INTO ${ident(op.table)} (${cols.map(ident).join(',')})
         VALUES (${cols.map((_, i) => `$${i + 1}`).join(',')})`,
        cols.map(c => op.row[c] as JSONValue),
      );
      break;
    }
    case 'remove':
      await pg.unsafe(
        `DELETE FROM ${ident(op.table)}
         WHERE ${pk.map((c, i) => `${ident(c)}=$${i + 1}`).join(' AND ')}`,
        pk.map(c => op.row[c] as JSONValue),
      );
      break;
    case 'edit': {
      const setCols = Object.keys(op.new).filter(c => !pk.includes(c));
      await pg.unsafe(
        `UPDATE ${ident(op.table)}
         SET ${setCols.map((c, i) => `${ident(c)}=$${i + 1}`).join(',')}
         WHERE ${pk
           .map((c, i) => `${ident(c)}=$${setCols.length + i + 1}`)
           .join(' AND ')}`,
        [
          ...setCols.map(c => op.new[c]),
          ...pk.map(c => op.old[c]),
        ] as JSONValue[],
      );
      break;
    }
  }
}

// ---- the oracle (z2s → Postgres) ----

async function runOracle(pg: PostgresDB, ast: ReturnType<typeof astOf>) {
  const q = formatPgInternalConvert(compile(serverSchema, schema, ast));
  return extractZqlResult(await pg.unsafe(q.text, q.values as JSONValue[]));
}

function astOf(query: AnyQuery) {
  return asQueryInternals(query).ast;
}

// Round non-integers so float8 (Postgres) vs JS division (avg) never trips an
// exact-equality compare; integers and strings pass through untouched.
function canon(v: unknown): unknown {
  return typeof v === 'number' && !Number.isInteger(v)
    ? Math.round(v * 1e9) / 1e9
    : v;
}

/** A materialized query plus how to read its value and the oracle's. */
type Probe = {label: string; check: () => Promise<void>};

function scalarProbe(
  qd: QueryDelegateImpl,
  pg: PostgresDB,
  query: AnyQuery,
  label: string,
): Probe {
  const ast = astOf(query);
  const view = qd.materialize(query);
  return {
    label,
    check: async () => {
      const oracle = canon(await runOracle(pg, ast));
      expect(canon((view as {data: unknown}).data), label).toEqual(oracle);
    },
  };
}

function relProbe(
  qd: QueryDelegateImpl,
  pg: PostgresDB,
  query: AnyQuery,
  alias: string,
  label: string,
): Probe {
  const ast = astOf(query);
  const view = qd.materialize(query);
  const rows = (data: unknown): [string, unknown][] =>
    (data as Record<string, unknown>[])
      .map(r => [r.id as string, canon(r[alias])] as [string, unknown])
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  return {
    label,
    check: async () => {
      const oracle = rows(await runOracle(pg, ast));
      expect(rows((view as {data: unknown}).data), label).toEqual(oracle);
    },
  };
}

/** Check every probe against the oracle, then after each push. */
async function drive(
  pg: PostgresDB,
  sources: Record<string, Source>,
  probes: Probe[],
  ops: Op[],
): Promise<void> {
  for (const p of probes) {
    await p.check(); // initial (empty) state
  }
  for (const op of ops) {
    applyIvm(sources, op);
    await applyPg(pg, op);
    for (const p of probes) {
      await p.check();
    }
  }
}

function freshSources(): Record<string, Source> {
  const make = (name: keyof typeof schema.tables) =>
    createSource(
      lc,
      testLogConfig,
      name,
      schema.tables[name].columns,
      schema.tables[name].primaryKey,
    );
  return {
    issue: make('issue'),
    comment: make('comment'),
    label: make('label'),
    issueLabel: make('issueLabel'),
  };
}

// row constructors
const cmt = (id: string, issueId: string, points: number | null): Row => ({
  id,
  issueId,
  points,
});
const iss = (id: string): Row => ({id, title: `issue ${id}`});
const lbl = (id: string, weight: number | null): Row => ({id, weight});
const edge = (issueId: string, labelId: string): Row => ({issueId, labelId});

describe('aggregate push path vs Postgres oracle', () => {
  let pg: PostgresDB;

  beforeAll(async () => {
    pg = await testDBs.create('aggregate-oracle-test');
    await pg.unsafe(DDL);
  });
  afterAll(async () => {
    await testDBs.drop(pg);
  });

  async function reset(): Promise<{
    sources: Record<string, Source>;
    qd: QueryDelegateImpl;
  }> {
    await pg.unsafe(`TRUNCATE issue, comment, label, "issueLabel"`);
    const sources = freshSources();
    return {sources, qd: new QueryDelegateImpl({sources})};
  }

  test('top-level count/sum/avg/min/max over a sequence of pushes', async () => {
    const {sources, qd} = await reset();
    const base = () => newQuery(schema, 'comment') as unknown as AnyQuery;
    const probes = [
      scalarProbe(qd, pg, base().count(), 'count'),
      scalarProbe(qd, pg, base().sum('points'), 'sum'),
      scalarProbe(qd, pg, base().avg('points'), 'avg'),
      scalarProbe(qd, pg, base().min('points'), 'min'),
      scalarProbe(qd, pg, base().max('points'), 'max'),
    ];
    const ops: Op[] = [
      {kind: 'insert', table: 'comment', row: cmt('c1', '1', 10)},
      {kind: 'insert', table: 'comment', row: cmt('c2', '1', 20)},
      {kind: 'insert', table: 'comment', row: cmt('c3', '2', 5)},
      // edit away the current max (20 -> 3): non-invertible re-fetch
      {
        kind: 'edit',
        table: 'comment',
        old: cmt('c2', '1', 20),
        new: cmt('c2', '1', 3),
      },
      // remove the current min (5): non-invertible re-fetch
      {kind: 'remove', table: 'comment', row: cmt('c3', '2', 5)},
      // a null contributor: counts for count(*), ignored by the rest
      {kind: 'insert', table: 'comment', row: cmt('c4', '2', null)},
      // value -> null
      {
        kind: 'edit',
        table: 'comment',
        old: cmt('c1', '1', 10),
        new: cmt('c1', '1', null),
      },
      {kind: 'remove', table: 'comment', row: cmt('c4', '2', null)},
      {kind: 'remove', table: 'comment', row: cmt('c1', '1', null)},
      // empties the table: sum/avg/min/max -> null, count -> 0
      {kind: 'remove', table: 'comment', row: cmt('c2', '1', 3)},
    ];
    await drive(pg, sources, probes, ops);
  });

  test('relationship count/sum/avg/min/max over a sequence of pushes', async () => {
    const {sources, qd} = await reset();
    const base = () => newQuery(schema, 'issue') as unknown as AnyQuery;
    const probes = [
      relProbe(
        qd,
        pg,
        base().related('comments', (c: AnyQuery) => c.count()),
        'comments',
        'count',
      ),
      relProbe(
        qd,
        pg,
        base().related('comments', (c: AnyQuery) => c.sum('points')),
        'comments',
        'sum',
      ),
      relProbe(
        qd,
        pg,
        base().related('comments', (c: AnyQuery) => c.avg('points')),
        'comments',
        'avg',
      ),
      relProbe(
        qd,
        pg,
        base().related('comments', (c: AnyQuery) => c.min('points')),
        'comments',
        'min',
      ),
      relProbe(
        qd,
        pg,
        base().related('comments', (c: AnyQuery) => c.max('points')),
        'comments',
        'max',
      ),
    ];
    const ops: Op[] = [
      {kind: 'insert', table: 'issue', row: iss('1')},
      {kind: 'insert', table: 'issue', row: iss('2')},
      {kind: 'insert', table: 'issue', row: iss('3')},
      {kind: 'insert', table: 'comment', row: cmt('c1', '1', 10)},
      {kind: 'insert', table: 'comment', row: cmt('c2', '1', 20)},
      {kind: 'insert', table: 'comment', row: cmt('c3', '2', 5)},
      // edit away issue 1's max
      {
        kind: 'edit',
        table: 'comment',
        old: cmt('c2', '1', 20),
        new: cmt('c2', '1', 3),
      },
      // move c1 from issue 1 to issue 2 (correlation-key change → Join splits
      // into remove-from-1 + add-to-2)
      {
        kind: 'edit',
        table: 'comment',
        old: cmt('c1', '1', 10),
        new: cmt('c1', '2', 10),
      },
      // remove issue 2's min (c3=5)
      {kind: 'remove', table: 'comment', row: cmt('c3', '2', 5)},
      // value -> null in issue 2
      {
        kind: 'edit',
        table: 'comment',
        old: cmt('c1', '2', 10),
        new: cmt('c1', '2', null),
      },
      {kind: 'insert', table: 'comment', row: cmt('c4', '3', 7)},
      // drop a populated issue: its row disappears from the result
      {kind: 'remove', table: 'comment', row: cmt('c4', '3', 7)},
      {kind: 'remove', table: 'issue', row: iss('3')},
      // empty issue 1 and issue 2's groups
      {kind: 'remove', table: 'comment', row: cmt('c2', '1', 3)},
      {kind: 'remove', table: 'comment', row: cmt('c1', '2', null)},
    ];
    await drive(pg, sources, probes, ops);
  });

  test('junction count/sum/avg/min/max over a sequence of pushes', async () => {
    const {sources, qd} = await reset();
    const base = () => newQuery(schema, 'issue') as unknown as AnyQuery;
    const probes = [
      relProbe(
        qd,
        pg,
        base().related('labels', (l: AnyQuery) => l.count()),
        'labels',
        'count',
      ),
      relProbe(
        qd,
        pg,
        base().related('labels', (l: AnyQuery) => l.sum('weight')),
        'labels',
        'sum',
      ),
      relProbe(
        qd,
        pg,
        base().related('labels', (l: AnyQuery) => l.avg('weight')),
        'labels',
        'avg',
      ),
      relProbe(
        qd,
        pg,
        base().related('labels', (l: AnyQuery) => l.min('weight')),
        'labels',
        'min',
      ),
      relProbe(
        qd,
        pg,
        base().related('labels', (l: AnyQuery) => l.max('weight')),
        'labels',
        'max',
      ),
    ];
    const ops: Op[] = [
      {kind: 'insert', table: 'issue', row: iss('1')},
      {kind: 'insert', table: 'issue', row: iss('2')},
      {kind: 'insert', table: 'label', row: lbl('L1', 10)},
      {kind: 'insert', table: 'label', row: lbl('L2', 20)},
      {kind: 'insert', table: 'label', row: lbl('L3', 5)},
      {kind: 'insert', table: 'issueLabel', row: edge('1', 'L1')},
      {kind: 'insert', table: 'issueLabel', row: edge('1', 'L2')},
      {kind: 'insert', table: 'issueLabel', row: edge('2', 'L3')},
      // edit a destination field through the junction (arrives as a CHILD change
      // on the edge → LiftField turns it into an edit of the lifted column);
      // also edits away issue 1's max (20 -> 3)
      {kind: 'edit', table: 'label', old: lbl('L2', 20), new: lbl('L2', 3)},
      // remove an edge → issue 2's only label gone (group empties)
      {kind: 'remove', table: 'issueLabel', row: edge('2', 'L3')},
      // a destination weight -> null
      {kind: 'edit', table: 'label', old: lbl('L1', 10), new: lbl('L1', null)},
      // remove a label that still has an edge: count still counts the edge; the
      // value aggregates drop it (inner join / null), and both sides agree
      {kind: 'remove', table: 'label', row: lbl('L1', null)},
      // add a fresh edge+label to issue 2
      {kind: 'insert', table: 'label', row: lbl('L4', 8)},
      {kind: 'insert', table: 'issueLabel', row: edge('2', 'L4')},
    ];
    await drive(pg, sources, probes, ops);
  });
});
