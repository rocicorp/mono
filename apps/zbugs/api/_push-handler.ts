import {
  PushProcessor,
  type DBConnection,
  type DBTransaction,
  type Params,
  type Row,
} from '@rocicorp/zero/pg';
import postgres, {type JSONValue} from 'postgres';
import {schema} from '../shared/schema.ts';
import {createServerMutators, type PostCommitTask} from './_server-mutators.ts';
import type {ReadonlyJSONObject} from '@rocicorp/zero';
import type {AuthData} from '../shared/auth.ts';

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

export async function handlePush(
  authData: AuthData | undefined,
  params: Params,
  body: ReadonlyJSONObject,
) {
  const postCommitTasks: PostCommitTask[] = [];
  const mutators = createServerMutators(authData, postCommitTasks);
  const processor = new PushProcessor(
    schema,
    () => new Connection(mutatorSql),
    mutators,
  );
  const response = await processor.process(params, body);
  await Promise.all(postCommitTasks.map(task => task()));
  return response;
}
