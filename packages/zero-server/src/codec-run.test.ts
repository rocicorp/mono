import {afterEach, assert, expect, test, vi} from 'vitest';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {Row} from '../../zero-protocol/src/data.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import type {Format} from '../../zero-types/src/format.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import type {TransactionMutate} from '../../zql/src/mutate/crud.ts';
import type {DBConnection, DBTransaction} from '../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import {CRUDMutatorFactory, TransactionImpl} from './custom.ts';
import {ZQLDatabase} from './zql-database.ts';

const schema = createSchema({
  enableLegacyQueries: true,
  tables: [
    table('event')
      .columns({
        id: string(),
        at: number().codec<Date>({
          decode: (n: number) => new Date(n),
          encode: (d: Date) => d.getTime(),
        }),
      })
      .primaryKey('id'),
  ],
});
const zql = createBuilder(schema);

// A custom adapter: its `runQuery` does its own SQL and returns codec columns
// in their stored form. Server reads must still decode them.
const storedRows: Row[] = [{id: 'a', at: 1000}];
// Like a real adapter, a singular format yields one row rather than an array.
const runQuery = vi.fn((_ast: AST, format: Format) => {
  const rows = structuredClone(storedRows);
  return Promise.resolve(format.singular ? rows[0] : rows);
});

function makeDBTransaction(): DBTransaction<unknown> {
  return {
    wrappedTransaction: {},
    query: () => Promise.resolve([]),
    runQuery,
  } as unknown as DBTransaction<unknown>;
}

afterEach(() => {
  vi.restoreAllMocks();
  runQuery.mockClear();
});

test('tx.run decodes codec columns from a custom runQuery', async () => {
  const tx = new TransactionImpl(
    makeDBTransaction(),
    'client',
    1,
    {} as TransactionMutate<typeof schema>,
    schema,
    {} as ServerSchema,
  );

  const rows = await tx.run(zql.event);
  expect(rows[0].at).toBeInstanceOf(Date);
  expect(rows[0].at.getTime()).toBe(1000);

  const one = await tx.run(zql.event.one());
  expect(one?.at).toBeInstanceOf(Date);
  expect(runQuery).toHaveBeenCalledTimes(2);
});

test('legacy tx.query decodes codec columns from a custom runQuery', async () => {
  const tx = new TransactionImpl(
    makeDBTransaction(),
    'client',
    1,
    {} as TransactionMutate<typeof schema>,
    schema,
    {} as ServerSchema,
  );

  // tx.query exists only for schemas with legacy queries enabled.
  const query = tx.query;
  assert(query);
  const rows = await query.event.run();
  expect(rows[0].at).toBeInstanceOf(Date);
  expect(rows[0].at.getTime()).toBe(1000);
});

test('ZQLDatabase.run decodes codec columns from a custom runQuery', async () => {
  vi.spyOn(
    CRUDMutatorFactory.prototype,
    'getOrFetchServerSchema',
  ).mockResolvedValue({} as ServerSchema);
  const connection = {
    transaction: () => {
      throw new Error('reads should use `query` when it is implemented');
    },
    query: () => Promise.resolve([]),
    runQuery,
  } as unknown as DBConnection<unknown>;

  const db = new ZQLDatabase(connection, schema);
  const rows = await db.run(zql.event);
  expect(rows[0].at).toBeInstanceOf(Date);
  expect(rows[0].at.getTime()).toBe(1000);
});
