/**
 * Executes z2s-compiled aggregate-relationship SQL (count/sum/avg) against a
 * real PostgreSQL instance and checks the returned scalars. This is the
 * end-to-end proof for the server-execution path (custom queries / runQuery):
 * the aggregate is computed in Postgres and no child rows are materialized.
 */
import type {JSONValue} from 'postgres';
import {afterAll, beforeAll, describe, expect, test} from 'vitest';
import {testDBs} from '../../zero-cache/src/test/db.ts';
import type {PostgresDB} from '../../zero-cache/src/types/pg.ts';
import type {AST, Condition} from '../../zero-protocol/src/ast.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import {compile, extractZqlResult} from './compiler.ts';
import {formatPgInternalConvert} from './sql.ts';

const DB_NAME = 'compiler-aggregate-test';

const issue = table('issue')
  .columns({id: string(), title: string()})
  .primaryKey('id');

const comment = table('comment')
  .columns({id: string(), issueId: string(), points: number().optional()})
  .primaryKey('id');

const schema = createSchema({tables: [issue, comment]});

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
};

function aggQuery(
  fn: 'count' | 'sum' | 'avg' | 'min' | 'max',
  field?: string,
): AST {
  return {
    table: 'issue',
    related: [
      {
        correlation: {parentField: ['id'], childField: ['issueId']},
        aggregate: {fn, field},
        subquery: {table: 'comment', alias: 'agg'},
      },
    ],
  };
}

async function run(pg: PostgresDB, ast: AST) {
  const q = formatPgInternalConvert(compile(serverSchema, schema, ast));
  return extractZqlResult(await pg.unsafe(q.text, q.values as JSONValue[]));
}

describe('aggregate relationships against PostgreSQL', () => {
  let pg: PostgresDB;

  beforeAll(async () => {
    pg = await testDBs.create(DB_NAME);
    await pg.unsafe(`
      CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT NOT NULL);
      CREATE TABLE comment (
        id TEXT PRIMARY KEY, "issueId" TEXT NOT NULL, points INT);

      INSERT INTO issue (id, title) VALUES
        ('1', 'issue 1'), ('2', 'issue 2'), ('3', 'issue 3');

      -- issue 1 -> points 10,20 ; issue 2 -> 5 ; issue 3 -> none
      INSERT INTO comment (id, "issueId", points) VALUES
        ('c1', '1', 10), ('c2', '1', 20), ('c3', '2', 5);
    `);
  });

  afterAll(async () => {
    await testDBs.drop(pg);
  });

  test('count(*) — empty group is 0', async () => {
    expect(await run(pg, aggQuery('count'))).toEqual([
      {id: '1', title: 'issue 1', agg: 2},
      {id: '2', title: 'issue 2', agg: 1},
      {id: '3', title: 'issue 3', agg: 0},
    ]);
  });

  test('sum(field) — empty group is null', async () => {
    expect(await run(pg, aggQuery('sum', 'points'))).toEqual([
      {id: '1', title: 'issue 1', agg: 30},
      {id: '2', title: 'issue 2', agg: 5},
      {id: '3', title: 'issue 3', agg: null},
    ]);
  });

  test('avg(field) — empty group is null', async () => {
    expect(await run(pg, aggQuery('avg', 'points'))).toEqual([
      {id: '1', title: 'issue 1', agg: 15},
      {id: '2', title: 'issue 2', agg: 5},
      {id: '3', title: 'issue 3', agg: null},
    ]);
  });

  test('min(field) — empty group is null', async () => {
    expect(await run(pg, aggQuery('min', 'points'))).toEqual([
      {id: '1', title: 'issue 1', agg: 10},
      {id: '2', title: 'issue 2', agg: 5},
      {id: '3', title: 'issue 3', agg: null},
    ]);
  });

  test('max(field) — empty group is null', async () => {
    expect(await run(pg, aggQuery('max', 'points'))).toEqual([
      {id: '1', title: 'issue 1', agg: 20},
      {id: '2', title: 'issue 2', agg: 5},
      {id: '3', title: 'issue 3', agg: null},
    ]);
  });
});

describe('top-level (ungrouped) aggregates against PostgreSQL', () => {
  let pg: PostgresDB;
  beforeAll(async () => {
    pg = await testDBs.create('compiler-toplevel-agg-test');
    await pg.unsafe(`
      CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT NOT NULL);
      CREATE TABLE comment (
        id TEXT PRIMARY KEY, "issueId" TEXT NOT NULL, points INT);
      INSERT INTO issue (id, title) VALUES ('1','a'),('2','b'),('3','c');
      INSERT INTO comment (id, "issueId", points) VALUES
        ('c1','1',10),('c2','1',20),('c3','2',5);
    `);
  });
  afterAll(async () => {
    await testDBs.drop(pg);
  });

  async function run1(ast: AST) {
    const q = formatPgInternalConvert(compile(serverSchema, schema, ast));
    return extractZqlResult(await pg.unsafe(q.text, q.values as JSONValue[]));
  }

  test('count(*) of a table is a scalar', async () => {
    expect(await run1({table: 'issue', aggregate: {fn: 'count'}})).toBe(3);
  });

  test('sum(field) of a table is a scalar', async () => {
    expect(
      await run1({table: 'comment', aggregate: {fn: 'sum', field: 'points'}}),
    ).toBe(35);
  });

  test('max(field) of a table is a scalar', async () => {
    expect(
      await run1({table: 'comment', aggregate: {fn: 'max', field: 'points'}}),
    ).toBe(20);
  });
});

describe('junction (many-to-many) aggregates against PostgreSQL', () => {
  const jSchema = createSchema({
    tables: [
      table('issue').columns({id: string(), title: string()}).primaryKey('id'),
      table('label')
        .columns({id: string(), name: string(), weight: number().optional()})
        .primaryKey('id'),
      table('issueLabel')
        .columns({issueId: string(), labelId: string()})
        .primaryKey('issueId', 'labelId'),
    ],
  });
  const jServerSchema: ServerSchema = {
    issue: {
      id: {type: 'text', isArray: false, isEnum: false},
      title: {type: 'text', isArray: false, isEnum: false},
    },
    label: {
      id: {type: 'text', isArray: false, isEnum: false},
      name: {type: 'text', isArray: false, isEnum: false},
      weight: {type: 'numeric', isArray: false, isEnum: false},
    },
    issueLabel: {
      issueId: {type: 'text', isArray: false, isEnum: false},
      labelId: {type: 'text', isArray: false, isEnum: false},
    },
  };

  // issue -> issueLabel -> label, aggregating label.<field> per issue. An
  // optional `destWhere` filters the destination (label) before aggregating.
  function junctionAgg(
    fn: 'sum' | 'avg' | 'min' | 'max',
    field: string,
    destWhere?: Condition,
  ): AST {
    return {
      table: 'issue',
      related: [
        {
          correlation: {parentField: ['id'], childField: ['issueId']},
          aggregate: {fn, field},
          subquery: {
            table: 'issueLabel',
            alias: 'labels',
            related: [
              {
                correlation: {parentField: ['labelId'], childField: ['id']},
                subquery: {
                  table: 'label',
                  alias: 'labels',
                  ...(destWhere ? {where: destWhere} : null),
                },
              },
            ],
          },
        },
      ],
    };
  }

  // count over the junction. A `destWhere` becomes an EXISTS on the junction row
  // (keep only edges whose label matches), so the count never touches the label.
  function junctionCount(destWhere?: Condition): AST {
    return {
      table: 'issue',
      related: [
        {
          correlation: {parentField: ['id'], childField: ['issueId']},
          aggregate: {fn: 'count'},
          subquery: {
            table: 'issueLabel',
            alias: 'labels',
            ...(destWhere
              ? {
                  where: {
                    type: 'correlatedSubquery',
                    op: 'EXISTS',
                    related: {
                      correlation: {
                        parentField: ['labelId'],
                        childField: ['id'],
                      },
                      subquery: {
                        table: 'label',
                        alias: 'labels',
                        where: destWhere,
                      },
                    },
                  },
                }
              : null),
          },
        },
      ],
    };
  }

  const weightGt5: Condition = {
    type: 'simple',
    op: '>',
    left: {type: 'column', name: 'weight'},
    right: {type: 'literal', value: 5},
  };
  const nameIsBug: Condition = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'name'},
    right: {type: 'literal', value: 'bug'},
  };

  let pg: PostgresDB;
  beforeAll(async () => {
    pg = await testDBs.create('compiler-junction-agg-test');
    await pg.unsafe(`
      CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT NOT NULL);
      CREATE TABLE label (id TEXT PRIMARY KEY, name TEXT NOT NULL, weight INT);
      CREATE TABLE "issueLabel" (
        "issueId" TEXT NOT NULL, "labelId" TEXT NOT NULL,
        PRIMARY KEY ("issueId", "labelId"));

      INSERT INTO issue (id, title) VALUES ('1','i1'),('2','i2'),('3','i3');
      INSERT INTO label (id, name, weight) VALUES
        ('L1','bug',10),('L2','feat',20),('L3','wont',5);
      -- issue 1 -> {L1(10), L2(20)} ; issue 2 -> {L3(5)} ; issue 3 -> {}
      INSERT INTO "issueLabel" ("issueId","labelId") VALUES
        ('1','L1'),('1','L2'),('2','L3');
    `);
  });
  afterAll(async () => {
    await testDBs.drop(pg);
  });

  async function run(ast: AST) {
    const q = formatPgInternalConvert(compile(jServerSchema, jSchema, ast));
    return extractZqlResult(await pg.unsafe(q.text, q.values as JSONValue[]));
  }

  test('sum over the destination field', async () => {
    expect(await run(junctionAgg('sum', 'weight'))).toEqual([
      {id: '1', title: 'i1', labels: 30},
      {id: '2', title: 'i2', labels: 5},
      {id: '3', title: 'i3', labels: null},
    ]);
  });

  test('avg over the destination field', async () => {
    expect(await run(junctionAgg('avg', 'weight'))).toEqual([
      {id: '1', title: 'i1', labels: 15},
      {id: '2', title: 'i2', labels: 5},
      {id: '3', title: 'i3', labels: null},
    ]);
  });

  test('min over the destination field', async () => {
    expect(await run(junctionAgg('min', 'weight'))).toEqual([
      {id: '1', title: 'i1', labels: 10},
      {id: '2', title: 'i2', labels: 5},
      {id: '3', title: 'i3', labels: null},
    ]);
  });

  test('max over the destination field', async () => {
    expect(await run(junctionAgg('max', 'weight'))).toEqual([
      {id: '1', title: 'i1', labels: 20},
      {id: '2', title: 'i2', labels: 5},
      {id: '3', title: 'i3', labels: null},
    ]);
  });

  // --- with a `where` on the destination (label). weight > 5 excludes L3(5),
  // so issue 2's only label drops out (empty group -> null). ---
  test('sum over the destination field with a where', async () => {
    expect(await run(junctionAgg('sum', 'weight', weightGt5))).toEqual([
      {id: '1', title: 'i1', labels: 30}, // L1(10) + L2(20)
      {id: '2', title: 'i2', labels: null}, // L3(5) filtered out
      {id: '3', title: 'i3', labels: null},
    ]);
  });

  test('avg over the destination field with a where', async () => {
    expect(await run(junctionAgg('avg', 'weight', weightGt5))).toEqual([
      {id: '1', title: 'i1', labels: 15},
      {id: '2', title: 'i2', labels: null},
      {id: '3', title: 'i3', labels: null},
    ]);
  });

  test('min over the destination field with a where', async () => {
    expect(await run(junctionAgg('min', 'weight', weightGt5))).toEqual([
      {id: '1', title: 'i1', labels: 10},
      {id: '2', title: 'i2', labels: null},
      {id: '3', title: 'i3', labels: null},
    ]);
  });

  test('max over the destination field with a where', async () => {
    expect(await run(junctionAgg('max', 'weight', weightGt5))).toEqual([
      {id: '1', title: 'i1', labels: 20},
      {id: '2', title: 'i2', labels: null},
      {id: '3', title: 'i3', labels: null},
    ]);
  });

  test('count over the junction (no filter) counts edges', async () => {
    expect(await run(junctionCount())).toEqual([
      {id: '1', title: 'i1', labels: 2},
      {id: '2', title: 'i2', labels: 1},
      {id: '3', title: 'i3', labels: 0},
    ]);
  });

  test('count over the junction with a where (EXISTS on the edge)', async () => {
    // Only edges whose label is named 'bug' (L1) count.
    expect(await run(junctionCount(nameIsBug))).toEqual([
      {id: '1', title: 'i1', labels: 1}, // L1(bug); L2(feat) excluded
      {id: '2', title: 'i2', labels: 0}, // L3(wont) excluded
      {id: '3', title: 'i3', labels: 0},
    ]);
  });
});
