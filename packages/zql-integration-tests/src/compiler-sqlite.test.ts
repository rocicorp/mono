import {afterAll, beforeAll, expect, test} from 'vitest';
import type {JSONValue} from '../../shared/src/json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {compileSQLite, extractZqlResult} from '../../z2s/src/compiler.ts';
import {formatSqliteInternalConvert} from '../../z2s/src/sql.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import {MemorySource} from '../../zql/src/ivm/memory-source.ts';
import {consume} from '../../zql/src/ivm/stream.ts';
import {makeSourceChangeAdd} from '../../zql/src/ivm/source.ts';
import {newQuery} from '../../zql/src/query/query-impl.ts';
import {asQueryInternals} from '../../zql/src/query/query-internals.ts';
import type {AnyQuery} from '../../zql/src/query/query.ts';
import {QueryDelegateImpl as TestMemoryQueryDelegate} from '../../zql/src/query/test/query-delegate.ts';
import {Database} from '../../zqlite/src/db.ts';
import './helpers/comparePg.ts';


const project = table('project')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const task = table('task')
  .columns({
    id: string(),
    projectId: string().from('project_id'),
    assigneeId: string().from('assignee_id'),
    title: string(),
  })
  .primaryKey('id');

const assignee = table('assignee')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const projectRelationships = relationships(project, ({many}) => ({
  tasks: many({
    sourceField: ['id'],
    destField: ['projectId'],
    destSchema: task,
  }),
}));

const taskRelationships = relationships(task, ({one}) => ({
  project: one({
    sourceField: ['projectId'],
    destField: ['id'],
    destSchema: project,
  }),
  assignee: one({
    sourceField: ['assigneeId'],
    destField: ['id'],
    destSchema: assignee,
  }),
}));

const assigneeRelationships = relationships(assignee, ({many}) => ({
  tasks: many({
    sourceField: ['id'],
    destField: ['assigneeId'],
    destSchema: task,
  }),
}));

const schema = createSchema({
  tables: [project, task, assignee],
  relationships: [
    projectRelationships,
    taskRelationships,
    assigneeRelationships,
  ],
});

const createTableSQL = /*sql*/ `
CREATE TABLE "project" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL
);

CREATE TABLE "task" (
  "id" TEXT PRIMARY KEY,
  "project_id" TEXT NOT NULL,
  "assignee_id" TEXT NOT NULL,
  "title" TEXT NOT NULL
);

CREATE TABLE "assignee" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL
);
`;

const serverSchema: ServerSchema = {
  project: {
    id: {type: 'text', isEnum: false, isArray: false},
    name: {type: 'text', isEnum: false, isArray: false},
  },
  task: {
    id: {type: 'text', isEnum: false, isArray: false},
    project_id: {type: 'text', isEnum: false, isArray: false},
    assignee_id: {type: 'text', isEnum: false, isArray: false},
    title: {type: 'text', isEnum: false, isArray: false},
  },
  assignee: {
    id: {type: 'text', isEnum: false, isArray: false},
    name: {type: 'text', isEnum: false, isArray: false},
  },
};

const testData: Record<string, Row[]> = {
  project: [
    {id: 'project1', name: 'Zero'},
    {id: 'project2', name: 'Replicache'},
  ],
  task: [
    {
      id: 'task1',
      projectId: 'project1',
      assigneeId: 'user1',
      title: 'Write SQLite compiler',
    },
    {
      id: 'task2',
      projectId: 'project1',
      assigneeId: 'user2',
      title: 'Add oracle tests',
    },
    {
      id: 'task3',
      projectId: 'project2',
      assigneeId: 'user2',
      title: 'Review SQL snapshots',
    },
  ],
  assignee: [
    {id: 'user1', name: 'Alice'},
    {id: 'user2', name: 'Bob'},
  ],
};

let sqlite: Database;
let memoryDelegate: TestMemoryQueryDelegate;

beforeAll(() => {
  sqlite = new Database(createSilentLogContext(), ':memory:');
  sqlite.exec(createTableSQL);
  seedSQLite();

  const sources: Record<string, MemorySource> = Object.fromEntries(
    Object.entries(schema.tables).map(([key, tableSchema]) => [
      key,
      new MemorySource(
        tableSchema.name,
        tableSchema.columns,
        tableSchema.primaryKey,
      ),
    ]),
  );
  memoryDelegate = new TestMemoryQueryDelegate({sources});

  for (const [table, rows] of Object.entries(testData)) {
    for (const row of rows) {
      consume(sources[table].push(makeSourceChangeAdd(row)));
    }
  }
});

function seedSQLite() {
  const insertProject = sqlite.prepare(
    'INSERT INTO "project" ("id", "name") VALUES (?, ?)',
  );
  for (const row of testData.project) {
    insertProject.run(row.id, row.name);
  }

  const insertTask = sqlite.prepare(
    'INSERT INTO "task" ("id", "project_id", "assignee_id", "title") VALUES (?, ?, ?, ?)',
  );
  for (const row of testData.task) {
    insertTask.run(row.id, row.projectId, row.assigneeId, row.title);
  }

  const insertAssignee = sqlite.prepare(
    'INSERT INTO "assignee" ("id", "name") VALUES (?, ?)',
  );
  for (const row of testData.assignee) {
    insertAssignee.run(row.id, row.name);
  }
}

afterAll(() => {
  sqlite.close();
});

test('basic query matches ZQL', async () => {
  await expectSQLiteCompilerToMatchZql(
    newQuery(schema, 'task')
      .where('assigneeId', 'IN', ['user1', 'user2'])
      .where('title', 'LIKE', '%SQL%')
      .orderBy('title', 'asc'),
  );
});

test('many relationship query matches ZQL', async () => {
  await expectSQLiteCompilerToMatchZql(
    newQuery(schema, 'project')
      .where('id', 'project1')
      .related('tasks', q => q.orderBy('title', 'asc')),
  );
});

test('one relationship query matches ZQL', async () => {
  await expectSQLiteCompilerToMatchZql(
    newQuery(schema, 'task')
      .related('project')
      .related('assignee')
      .where('id', 'task1'),
  );
});

async function expectSQLiteCompilerToMatchZql(query: AnyQuery) {
  const queryInternals = asQueryInternals(query);
  const sqlQuery = formatSqliteInternalConvert(
    compileSQLite(
      serverSchema,
      schema,
      queryInternals.ast,
      queryInternals.format,
    ),
  );
  const sqliteResult = extractZqlResult(
    sqlite
      .prepare(sqlQuery.text)
      .all(...(sqlQuery.values as JSONValue[])),
  );
  const zqlResult = await memoryDelegate.run(query);

  expect(sqliteResult).toEqualPg(zqlResult);
}
