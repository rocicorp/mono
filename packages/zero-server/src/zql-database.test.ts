import {describe, expect, test, vi} from 'vitest';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../zero-schema/src/builder/table-builder.ts';
import type {DBConnection, DBTransaction} from '../../zql/src/mutate/custom.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import {ZQLDatabase} from './zql-database.ts';

const schema = createSchema({
  tables: [table('foo').columns({id: string()}).primaryKey('id')],
});

const builder = createBuilder(schema);

// A `pg_catalog` row describing `foo.id` as a `text` column, matching the
// shape `getServerSchema` expects back from its catalog query.
const catalogRow = {
  schema: 'public',
  table: 'foo',
  column: 'id',
  dataType: 'text',
  length: null,
  precision: null,
  scale: null,
  typtype: 'b',
  typename: 'text',
  elemTyptype: null,
  elemTypname: null,
};

const isCatalogQuery = (text: string) => text.includes('pg_catalog');

/**
 * A `DBConnection` whose `query` answers the server schema lookup with
 * `catalogRow` and every other statement with an empty ZQL result.
 */
function fakeConnection(extra: object = {}) {
  const query = vi.fn((text: string) =>
    Promise.resolve(isCatalogQuery(text) ? [catalogRow] : [{zql_result: '[]'}]),
  );
  const transaction = vi.fn();
  const connection = {
    query,
    transaction,
    ...extra,
  } as unknown as DBConnection<unknown>;
  return {connection, query, transaction};
}

function catalogQueries(query: ReturnType<typeof fakeConnection>['query']) {
  return query.mock.calls.filter(([text]) => isCatalogQuery(text)).length;
}

describe('ZQLDatabase.run', () => {
  test('runs without a transaction when the connection supports query', async () => {
    const {connection, query, transaction} = fakeConnection();

    const zql = new ZQLDatabase(connection, schema);
    const result = await zql.run(builder.foo);

    expect(result).toEqual([]);
    expect(transaction).not.toHaveBeenCalled();
    // One server schema lookup plus the compiled select.
    expect(query).toHaveBeenCalledTimes(2);
    expect(catalogQueries(query)).toBe(1);
  });

  test('caches the server schema across repeated calls to run', async () => {
    const {connection, query} = fakeConnection();

    const zql = new ZQLDatabase(connection, schema);
    await zql.run(builder.foo);
    await zql.run(builder.foo);

    expect(catalogQueries(query)).toBe(1);
  });

  test('concurrent cold runs share one server schema fetch', async () => {
    const {connection, query} = fakeConnection();

    const zql = new ZQLDatabase(connection, schema);
    await Promise.all([
      zql.run(builder.foo),
      zql.run(builder.foo),
      zql.run(builder.foo),
    ]);

    expect(catalogQueries(query)).toBe(1);
  });

  test('uses the connection runQuery when provided', async () => {
    const runQuery = vi.fn(() => Promise.resolve([{id: 'x'}]));
    const {connection, query} = fakeConnection({runQuery});

    const zql = new ZQLDatabase(connection, schema);
    const result = await zql.run(builder.foo);

    expect(result).toEqual([{id: 'x'}]);
    expect(runQuery).toHaveBeenCalledTimes(1);
    // Only the server schema lookup hits `query`; the select goes through
    // `runQuery`.
    expect(query).toHaveBeenCalledTimes(1);
  });

  test('falls back to a transaction when the connection has no query', async () => {
    const dbTxQuery = vi.fn(() => Promise.resolve([catalogRow]));
    const dbTxRunQuery = vi.fn(() => Promise.resolve([] as unknown[]));
    const dbTx = {
      wrappedTransaction: null,
      query: dbTxQuery,
      runQuery: dbTxRunQuery,
    } as unknown as DBTransaction<unknown>;

    const connectionTransaction = vi.fn(
      (fn: (tx: DBTransaction<unknown>) => Promise<unknown>) => fn(dbTx),
    );

    const connection = {
      transaction: connectionTransaction,
    } as unknown as DBConnection<unknown>;

    const zql = new ZQLDatabase(connection, schema);
    const result = await zql.run(builder.foo);

    expect(result).toEqual([]);
    expect(connectionTransaction).toHaveBeenCalledTimes(1);
    expect(dbTxQuery).toHaveBeenCalledTimes(1);
    expect(dbTxRunQuery).toHaveBeenCalledTimes(1);
  });
});
