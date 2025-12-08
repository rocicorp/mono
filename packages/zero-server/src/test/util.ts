import type {JSONValue} from '../../../shared/src/json.ts';
import type {PostgresTransaction} from '../../../zero-cache/src/types/pg.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Format} from '../../../zero-types/src/format.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../../zero-types/src/server-schema.ts';
import type {DBTransaction, Row} from '../../../zql/src/mutate/custom.ts';
import type {HumanReadable} from '../../../zql/src/query/query-builder.ts';
import {executePostgresQuery} from '../pg-query-executor.ts';

export class Transaction implements DBTransaction<PostgresTransaction> {
  readonly wrappedTransaction: PostgresTransaction;
  constructor(pgTx: PostgresTransaction) {
    this.wrappedTransaction = pgTx;
  }
  runQuery<TReturn>(
    ast: AST,
    format: Format,
    schema: Schema,
    serverSchema: ServerSchema,
  ): Promise<HumanReadable<TReturn>> {
    return executePostgresQuery<TReturn>(
      this,
      ast,
      format,
      schema,
      serverSchema,
    );
  }

  query(sql: string, params: unknown[]): Promise<Row[]> {
    return this.wrappedTransaction.unsafe(sql, params as JSONValue[]);
  }
}
