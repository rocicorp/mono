import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import type {DBTransaction, SchemaQuery} from '../../zql/src/mutate/custom.ts';
import {
  AbstractQuery,
  defaultFormat,
  newQuerySymbol,
} from '../../zql/src/query/query-impl.ts';
import type {HumanReadable, PullRow, Query} from '../../zql/src/query/query.ts';
import type {TypedView} from '../../zql/src/query/typed-view.ts';

export function makeSchemaQuery<S extends Schema>(
  schema: S,
): (
  dbTransaction: DBTransaction<unknown>,
  serverSchema: ServerSchema,
) => SchemaQuery<S> {
  class SchemaQueryHandler {
    readonly #dbTransaction: DBTransaction<unknown>;
    readonly #serverSchema: ServerSchema;
    constructor(
      dbTransaction: DBTransaction<unknown>,
      serverSchema: ServerSchema,
    ) {
      this.#dbTransaction = dbTransaction;
      this.#serverSchema = serverSchema;
    }

    get(
      target: Record<
        string,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        Omit<Query<S, string, any>, 'materialize' | 'preload'>
      >,
      prop: string,
    ) {
      if (prop in target) {
        return target[prop];
      }

      const q = new ZPGQuery(
        schema,
        this.#serverSchema,
        prop,
        this.#dbTransaction,
        {table: prop},
        defaultFormat,
      );
      target[prop] = q;
      return q;
    }
  }

  return (dbTransaction: DBTransaction<unknown>, serverSchema: ServerSchema) =>
    new Proxy(
      {},
      new SchemaQueryHandler(dbTransaction, serverSchema),
    ) as SchemaQuery<S>;
}

export class ZPGQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends AbstractQuery<TSchema, TTable, TReturn> {
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

  protected [newQuerySymbol]<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
  ): ZPGQuery<TSchema, TTable, TReturn> {
    return new ZPGQuery(
      schema,
      this.#serverSchema,
      tableName,
      this.#dbTransaction,
      ast,
      format,
    );
  }

  async run(): Promise<HumanReadable<TReturn>> {
    const sqlQuery =
      this.#query ??
      formatPgInternalConvert(
        compile(
          this.#serverSchema,
          this.#schema,
          this._completeAst(),
          this.format,
        ),
      );
    this.#query = sqlQuery;
    const pgIterableResult = await this.#dbTransaction.query(
      sqlQuery.text,
      sqlQuery.values,
    );

    const pgArrayResult = Array.isArray(pgIterableResult)
      ? pgIterableResult
      : [...pgIterableResult];
    if (pgArrayResult.length === 0 && this.format.singular) {
      return undefined as unknown as HumanReadable<TReturn>;
    }

    return extractZqlResult(pgArrayResult) as HumanReadable<TReturn>;
  }

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
