import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../zero-protocol/src/client-schema.ts';
import {TableSource} from '../../../zqlite/src/table-source.ts';
import type {NormalizedZeroConfig} from '../config/normalize.ts';
import {DbFile, initDB} from '../test/lite.ts';
import {analyzeQuery} from './analyze.ts';
import {CREATE_TABLE_METADATA_TABLE} from './replicator/schema/table-metadata.ts';

// Not in analyze.test.ts, which mocks TableSource and runAst for the whole
// file. This checks that the kill switch reaches the pipeline that `analyze`
// builds.

const lc = createSilentLogContext();

let dbFile: DbFile;

beforeEach(() => {
  dbFile = new DbFile('analyze_pushdown');
  const db = dbFile.connect(lc);
  initDB(
    db,
    /*sql*/ `
    CREATE TABLE issues (id TEXT PRIMARY KEY);
    CREATE TABLE comments (id TEXT PRIMARY KEY, "issueID" TEXT);
    ${CREATE_TABLE_METADATA_TABLE}
    `,
    {
      issues: [{id: '1'}, {id: '2'}],
      comments: [
        {id: '10', issueID: '1'},
        {id: '20', issueID: '2'},
      ],
    },
  );
  db.close();
});

afterEach(() => {
  vi.restoreAllMocks();
  dbFile.delete();
});

const clientSchema: ClientSchema = {
  tables: {
    issues: {columns: {id: {type: 'string'}}, primaryKey: ['id']},
    comments: {
      columns: {id: {type: 'string'}, issueID: {type: 'string'}},
      primaryKey: ['id'],
    },
  },
};

const ast: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  where: {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'id'},
    right: {type: 'literal', value: '1'},
  },
  related: [
    {
      correlation: {parentField: ['id'], childField: ['issueID']},
      subquery: {
        table: 'comments',
        alias: 'comments',
        orderBy: [['id', 'asc']],
      },
    },
  ],
};

test.each([
  ['default', undefined, true],
  ['on', true, true],
  ['off', false, false],
] as const)(
  'the correlated predicate pushdown flag reaches analyze (%s)',
  async (_, enabled, pushed) => {
    const connect = vi.spyOn(TableSource.prototype, 'connect');
    const config = {
      replica: {file: dbFile.path},
      log: testLogConfig,
      enableQueryPlanner: false,
      enableCorrelatedPredicatePushdown: enabled,
    } as NormalizedZeroConfig;

    const result = await analyzeQuery(lc, config, clientSchema, ast);

    expect(result.syncedRows).toEqual({
      issues: [{id: '1'}],
      comments: [{id: '10', issueID: '1'}],
    });
    const commentsFilters = connect.mock.calls
      .filter(
        (_, i) =>
          (connect.mock.contexts[i] as TableSource).tableSchema.name ===
          'comments',
      )
      .map(([, filters]) => filters);
    expect(commentsFilters).toEqual([
      pushed
        ? {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'issueID'},
            right: {type: 'literal', value: '1'},
          }
        : undefined,
    ]);
  },
);
