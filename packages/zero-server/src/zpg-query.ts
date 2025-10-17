import type {AST} from '../../zero-protocol/src/ast.ts';
import type {ServerSchema} from '../../zero-schema/src/server-schema.ts';
import type {Format} from '../../zero-types/src/format.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {DBTransaction} from '../../zql/src/mutate/custom.ts';
import type {CustomQueryID} from '../../zql/src/query/named.ts';
import type {QueryDelegate} from '../../zql/src/query/query-delegate.ts';
import {AbstractQuery} from '../../zql/src/query/query-impl.ts';
import type {HumanReadable, PullRow} from '../../zql/src/query/query.ts';
import type {TypedView} from '../../zql/src/query/typed-view.ts';

export class ZPGQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn = PullRow<TTable, TSchema>,
  TContext = unknown,
> extends AbstractQuery<TSchema, TTable, TReturn, TContext> {
  readonly #dbTransaction: DBTransaction<unknown>;
  readonly #serverSchema: ServerSchema;

  constructor(
    schema: TSchema,
    serverSchema: ServerSchema,
    tableName: TTable,
    dbTransaction: DBTransaction<unknown>,
    ast: AST,
    format: Format,
  ) {
    super(undefined, schema, tableName, ast, format, 'permissions', undefined);
    this.#dbTransaction = dbTransaction;
    this.#serverSchema = serverSchema;
  }

  protected _newQuerySymbol<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
  >(
    _delegate: QueryDelegate<TContext> | undefined,
    schema: TSchema,
    tableName: TTable,
    ast: AST,
    format: Format,
    _customQueryID: CustomQueryID | undefined,
    _currentJunction: string | undefined,
  ): AbstractQuery<TSchema, TTable, TReturn, TContext> {
    return new ZPGQuery<TSchema, TTable, TReturn, TContext>(
      schema,
      this.#serverSchema,
      tableName,
      this.#dbTransaction,
      ast,
      format,
    );
  }

  run(): Promise<HumanReadable<TReturn>> {
    throw new Error('Z2SQuery cannot be run');
    // return this.#queryRunner.run(this);
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
