import type {JSONValue} from '../../shared/src/json.ts';
import type {
  DBConnection,
  DBTransaction,
  Row,
} from '../../zql/src/mutate/custom.ts';

/**
 * Subset of the postgres lib's `Transaction` interface that we use.
 */
export type PostgresJSTransaction = {
  unsafe(sql: string, params: JSONValue[]): Promise<Row[]>;
};

/**
 * Subset of the postgres lib's `SQL` interface that we use.
 */
export type PostgresJSClient<Transaction extends PostgresJSTransaction> = {
  unsafe(sql: string, params: JSONValue[]): Promise<Row[]>;
  begin<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>;
};

class Transaction<WrappedTransaction extends PostgresJSTransaction>
  implements DBTransaction<WrappedTransaction>
{
  readonly wrappedTransaction: WrappedTransaction;
  constructor(pgTx: WrappedTransaction) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}

/**
 * Implements the `DBConnection` interface needed by PushProcessor for the
 * postgres.js library.
 */
export class PostgresJSConnection<
  WrappedTransaction extends PostgresJSTransaction,
  WrappedPostgres extends PostgresJSClient<WrappedTransaction>,
> implements DBConnection<WrappedTransaction>
{
  readonly #pg: WrappedPostgres;
  constructor(pg: WrappedPostgres) {
    this.#pg = pg;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#pg.unsafe(sql, params as JSONValue[]);
  }

  transaction<T>(
    fn: (tx: DBTransaction<WrappedTransaction>) => Promise<T>,
  ): Promise<T> {
    return this.#pg.begin(pgTx => fn(new Transaction(pgTx))) as Promise<T>;
  }
}
