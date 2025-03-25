import {first} from '../../shared/src/iterables.ts';
import {compile} from '../../z2s/src/compiler.ts';
import {formatPg} from '../../z2s/src/sql.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {Format} from '../../zql/src/ivm/view.ts';
import type {DBTransaction, SchemaQuery} from '../../zql/src/mutate/custom.ts';
import {AbstractQuery, defaultFormat} from '../../zql/src/query/query-impl.ts';
import type {HumanReadable, PullRow, Query} from '../../zql/src/query/query.ts';
import type {TTL} from '../../zql/src/query/ttl.ts';
import type {TypedView} from '../../zql/src/query/typed-view.ts';

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

      const q = new Z2SQuery(
        schema,
        prop,
        this.#dbTransaction,
        {table: prop},
        defaultFormat,
      );
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
  readonly #schema: TSchema;
  #query:
    | {
        text: string;
        values: unknown[];
      }
    | undefined;

  constructor(
    schema: TSchema,
    tableName: TTable,
    dbTransaction: DBTransaction<unknown>,
    ast: AST,
    format: Format,
  ) {
    super(schema, tableName, ast, format);
    this.#dbTransaction = dbTransaction;
    this.#schema = schema;
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
    format: Format,
  ): Z2SQuery<TSchema, TTable, TReturn> {
    return new Z2SQuery(schema, tableName, this.#dbTransaction, ast, format);
  }

  async run(): Promise<HumanReadable<TReturn>> {
    const sqlQuery =
      this.#query ??
      formatPg(compile(this._completeAst(), this.#schema.tables, this.format));
    this.#query = sqlQuery;
    const result = await this.#dbTransaction.query(
      sqlQuery.text,
      sqlQuery.values,
    );

    if (this.format.singular) {
      return first(result) as HumanReadable<TReturn>;
    }

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

  updateTTL(_ttl: TTL): void {
    throw new Error('Z2SQuery cannot have a TTL');
  }
}
