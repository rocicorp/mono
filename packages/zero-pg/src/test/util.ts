import type {DBTransaction, Row} from '../../../zql/src/mutate/custom.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import type {PostgresTransaction} from '../../../zero-cache/src/types/pg.ts';

export class Transaction implements DBTransaction<PostgresTransaction> {
  readonly wrappedTransaction: PostgresTransaction;
  constructor(pgTx: PostgresTransaction) {
    this.wrappedTransaction = pgTx;
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}
