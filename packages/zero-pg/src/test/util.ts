import type {
  DBConnection,
  DBTransaction,
  Row,
} from '../../../zql/src/mutate/custom.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {
  PostgresDB,
  PostgresTransaction,
} from '../../../zero-cache/src/types/pg.ts';

export class Connection implements DBConnection<PostgresTransaction> {
  readonly #pg: PostgresDB;
  constructor(pg: PostgresDB) {
    this.#pg = pg;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.#pg.unsafe(sql, params as JSONValue[]);
  }

  transaction<T>(
    fn: (tx: DBTransaction<PostgresTransaction>) => Promise<T>,
  ): Promise<T> {
    return this.#pg.begin(pgTx => fn(new Transaction(pgTx))) as Promise<T>;
  }
}

export class Transaction implements DBTransaction<PostgresTransaction> {
  readonly wrappedTransaction: PostgresTransaction;
  constructor(pgTx: PostgresTransaction) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}
