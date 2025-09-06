import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from '../../zqlite/src/db.ts';
import {testLogConfig} from '../../otel/src/test-log-config.ts';
import {newQueryDelegate} from '../../zqlite/src/test/source-factory.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  json,
  number,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../zql/src/query/named.ts';
import {runQuery} from '../../zql/src/query/run.ts';

test('reading from old array style tables', async () => {
  const lc = createSilentLogContext();
  const sqlite = new Database(lc, ':memory:');
  sqlite.exec(`CREATE TABLE "bar" ( 
    "id" "int4|NOT_NULL", 
    "bar" "text[]",       
    "baz" "int4[]",       
    "boo" "varchar[]",    
    "foo" "date[]",       
    "_0_version" "text",
    primary key ("id")
  );
  
  INSERT INTO "bar" ("id", "bar", "baz", "boo", "foo", "_0_version") VALUES
    (1, '["some", "string"]', '[1, 2]', '["ax", "21c"]', '[1234, 4321]', '4f58sg');
  `);

  const schema = createSchema({
    tables: [
      table('bar')
        .columns({
          id: number(),
          bar: json<string[]>(),
          baz: json<number[]>(),
          boo: json<string[]>(),
          foo: json<number[]>(),
        })
        .primaryKey('id'),
    ],
  });

  const d = newQueryDelegate(lc, testLogConfig, sqlite, schema);
  const queries = createBuilder(schema);

  const rows = await runQuery(d, queries.bar);
  expect(rows).toMatchInlineSnapshot(`
    [
      {
        "bar": [
          "some",
          "string",
        ],
        "baz": [
          1,
          2,
        ],
        "boo": [
          "ax",
          "21c",
        ],
        "foo": [
          1234,
          4321,
        ],
        "id": 1,
        Symbol(rc): 1,
      },
    ]
  `);
});
