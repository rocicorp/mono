import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import type {Format} from '../../zero-types/src/format.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {DBTransaction} from '../../zql/src/mutate/custom.ts';
import {AbstractQuery} from '../../zql/src/query/query-impl.ts';
import type {HumanReadable, PullRow} from '../../zql/src/query/query.ts';
import type {RunnableQuery} from '../../zql/src/query/runnable-query.ts';

export class ZPGQuery<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn = PullRow<TTable, TSchema>,
    TContext = unknown,
  >
  extends AbstractQuery<TSchema, TTable, TReturn, TContext>
  implements RunnableQuery<TSchema, TTable, TReturn, TContext>
{
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
    super(
      schema,
      tableName,
      ast,
      format,
      'permissions',
      undefined,
      undefined,
      (table, ast, format, _customQueryID, _currentJunction) =>
        new ZPGQuery(schema, serverSchema, table, dbTransaction, ast, format),
    );
    this.#dbTransaction = dbTransaction;
    this.#schema = schema;
    this.#serverSchema = serverSchema;
  }

  async run(): Promise<HumanReadable<TReturn>> {
    const sqlQuery =
      this.#query ??
      formatPgInternalConvert(
        compile(this.#serverSchema, this.#schema, this.ast, this.format),
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
}
