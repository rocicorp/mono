import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import type {Format} from '../../zero-types/src/format.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {DBTransaction} from '../../zql/src/mutate/custom.ts';
import type {CustomQueryID} from '../../zql/src/query/named.ts';
import {AbstractQuery} from '../../zql/src/query/query-impl.ts';
import type {
  HumanReadable,
  NoContext,
  PullRow,
} from '../../zql/src/query/query.ts';
import type {TypedView} from '../../zql/src/query/typed-view.ts';

export class ZPGQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
  TContext = NoContext,
> extends AbstractQuery<TSchema, TTable, TReturn, TContext> {
  readonly #dbTransaction: DBTransaction<unknown>;
  readonly #schema: TSchema;
  readonly #serverSchema: ServerSchema;

  #query:
    | {
        text: string;
        values: unknown[];
      }
    | undefined;

  constructor(
    schema: TSchema,
    serverSchema: ServerSchema,
    tableName: TTable,
    dbTransaction: DBTransaction<unknown>,
    ast: AST,
    format: Format,
  ) {
    super(schema, tableName, ast, format, 'permissions', undefined);
    this.#dbTransaction = dbTransaction;
    this.#schema = schema;
    this.#serverSchema = serverSchema;
  }

  protected _newQuerySymbol<
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    tableName: TTable,
    ast: AST,
    format: Format,
    _customQueryID: CustomQueryID | undefined,
    _currentJunction: string | undefined,
  ): AbstractQuery<TSchema, TTable, TReturn, TContext> {
    return new ZPGQuery(
      this.#schema,
      this.#serverSchema,
      tableName,
      this.#dbTransaction,
      ast,
      format,
    );
  }

  run(): Promise<HumanReadable<TReturn>> {
    return run<TReturn>(
      this.#query,
      this.#schema,
      this.#serverSchema,
      this.#dbTransaction,
      this.completedAST,
      this.format,
    );
  }

  // const sqlQuery =
  //   this.#query ??
  //   formatPgInternalConvert(
  //     compile(
  //       this.#serverSchema,
  //       this.#schema,
  //       this.completedAST,
  //       this.format,
  //     ),
  //   );
  // this.#query = sqlQuery;
  // const pgIterableResult = await this.#dbTransaction.query(
  //   sqlQuery.text,
  //   sqlQuery.values,
  // );

  // const pgArrayResult = Array.isArray(pgIterableResult)
  //   ? pgIterableResult
  //   : [...pgIterableResult];
  // if (pgArrayResult.length === 0 && this.format.singular) {
  //   return undefined as unknown as HumanReadable<TReturn>;
  // }

  // return extractZqlResult(pgArrayResult) as HumanReadable<TReturn>;
  // }

  preload(): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    throw new Error('Z2SQuery cannot be preloaded');
  }

  materialize(): TypedView<HumanReadable<TReturn>> {
    throw new Error('Z2SQuery cannot be materialized');
  }
}

async function run<TReturn>(
  query:
    | {
        text: string;
        values: unknown[];
      }
    | undefined,
  schema: Schema,
  serverSchema: ServerSchema,
  dbTransaction: DBTransaction<unknown>,
  completedAST: AST,
  format: Format,
): Promise<HumanReadable<TReturn>> {
  const sqlQuery =
    query ??
    formatPgInternalConvert(
      compile(serverSchema, schema, completedAST, format),
    );
  query = sqlQuery;
  const pgIterableResult = await dbTransaction.query(
    sqlQuery.text,
    sqlQuery.values,
  );

  const pgArrayResult = Array.isArray(pgIterableResult)
    ? pgIterableResult
    : [...pgIterableResult];
  if (pgArrayResult.length === 0 && format.singular) {
    return undefined as unknown as HumanReadable<TReturn>;
  }

  return extractZqlResult(pgArrayResult) as HumanReadable<TReturn>;
}
