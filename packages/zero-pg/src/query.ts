import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../../zql/src/mutate/custom.ts';
import type {DBTransaction} from './db.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import {AbstractQuery} from '../../zql/src/query/query-impl.ts';
import type {HumanReadable, PullRow, Query} from '../../zql/src/query/query.ts';
import type {TypedView} from '../../zql/src/query/typed-view.ts';
import {formatPg} from '../../z2s/src/sql.ts';
import {compile} from '../../z2s/src/compiler.ts';

export function makeSchemaQuery<S extends Schema>(
  schema: S,
): (dbTransaction: DBTransaction<unknown>) => SchemaQuery<S> {
  class SchemaQueryHandler {
    readonly #dbTransaction: DBTransaction<unknown>;
    constructor(dbTransaction: DBTransaction<unknown>) {
      this.#dbTransaction = dbTransaction;
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

      const q = new Z2SQuery(schema, prop, this.#dbTransaction);
      target[prop] = q;
      return q;
    }
  }

  return (dbTransaction: DBTransaction<unknown>) =>
    new Proxy({}, new SchemaQueryHandler(dbTransaction)) as SchemaQuery<S>;
}

export class Z2SQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
> extends AbstractQuery<TSchema, TTable, TReturn> {
  readonly #dbTransaction: DBTransaction<unknown>;

  constructor(
    schema: TSchema,
    tableName: TTable,
    dbTransaction: DBTransaction<unknown>,
    ast: AST = {table: tableName},
    format?: Format | undefined,
  ) {
    super(schema, tableName, ast, format);
    this.#dbTransaction = dbTransaction;
  }

  protected readonly _system = 'permissions';

  protected _newQuery<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format | undefined,
  ): Query<TSchema, TTable, TReturn> {
    return new Z2SQuery(schema, tableName, this.#dbTransaction, ast, format);
  }

  async run(): Promise<HumanReadable<TReturn>> {
    const sqlQuery = formatPg(compile(this._completeAst(), this.format));
    const result = await this.#dbTransaction.query(
      sqlQuery.text,
      sqlQuery.values,
    );
    if (Array.isArray(result)) {
      return result as HumanReadable<TReturn>;
    }
    return [...result] as HumanReadable<TReturn>;
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
