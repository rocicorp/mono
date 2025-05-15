import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {initialSync} from '../../../zero-cache/src/services/change-source/pg/initial-sync.ts';
import {getConnectionURI, testDBs} from '../../../zero-cache/src/test/db.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {clientToServer} from '../../../zero-schema/src/name-mapper.ts';
import {Database} from '../../../zqlite/src/db.ts';

export async function fillPgAndSync(
  schema: Schema,
  createTableSQL: string,
  testData: Record<string, Row[]>,
  dbName: string,
) {
  const lc = createSilentLogContext();
  const pg = await testDBs.create(dbName, undefined, false);

  await pg.unsafe(createTableSQL);
  const sqlite = new Database(lc, ':memory:');

  const mapper = clientToServer(schema.tables);
  for (const [table, rows] of Object.entries(testData)) {
    const columns = Object.keys(rows[0]);
    const forPg = rows.map(row =>
      columns.reduce(
        (acc, c) => ({
          ...acc,
          [mapper.columnName(table, c)]: row[c as keyof typeof row],
        }),
        {} as Record<string, unknown>,
      ),
    );
    await pg`INSERT INTO ${pg(mapper.tableName(table))} ${pg(forPg)}`;
  }

  await initialSync(
    lc,
    {appID: 'collate_test', shardNum: 0, publications: []},
    sqlite,
    getConnectionURI(pg),
    {tableCopyWorkers: 1},
  );

  return {pg, sqlite};
}
