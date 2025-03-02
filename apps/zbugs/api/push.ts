import {
  createPushHandler,
  type DBConnection,
  type DBTransaction,
  type PushHandler,
  type Row,
} from '@rocicorp/zero/pg';
import postgres, {type JSONValue} from 'postgres';
import {schema} from '../schema.ts';

class Connection implements DBConnection<postgres.TransactionSql> {
  readonly #pg: postgres.Sql;
  constructor(pg: postgres.Sql) {
    this.#pg = pg;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#pg.unsafe(sql, params as JSONValue[]);
  }

  transaction<T>(
    fn: (tx: DBTransaction<postgres.TransactionSql>) => Promise<T>,
  ): Promise<T> {
    return this.#pg.begin(pgTx => fn(new Transaction(pgTx))) as Promise<T>;
  }
}

class Transaction implements DBTransaction<postgres.TransactionSql> {
  readonly wrappedTransaction: postgres.TransactionSql;
  constructor(pgTx: postgres.TransactionSql) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}

const mutatorSql = postgres(process.env.ZERO_UPSTREAM_DB as string);

export const pushHandler: PushHandler = createPushHandler({
  dbConnectionProvider: () => new Connection(mutatorSql),
  mutators: {},
  schema,
  shardID: '0',
});
