import type {AST, System} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import {AbstractQuery} from './abstract-query.ts';
import type {CustomQueryID} from './named.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  PullRow,
  Query,
  RunOptions,
} from './query-builder.ts';
import type {QueryDelegate} from './query-delegate.ts';
import type {TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

export function newRunnableQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
>(
  delegate: QueryDelegate,
  schema: TSchema,
  table: TTable,
): Query<TTable, TSchema> {
  return new RunnableQueryImpl(
    delegate,
    schema,
    table,
    {table},
    defaultFormat,
    undefined,
  );
}

export class RunnableQueryImpl<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn = PullRow<TTable, TSchema>,
  >
  extends AbstractQuery<TTable, TSchema, TReturn>
  implements Query<TTable, TSchema, TReturn>
{
  readonly #delegate: QueryDelegate;

  constructor(
    delegate: QueryDelegate,
    schema: TSchema,
    tableName: TTable,
    ast: AST = {table: tableName},
    format: Format = defaultFormat,
    system: System = 'client',
    customQueryID?: CustomQueryID,
    currentJunction?: string,
  ) {
    super(
      schema,
      tableName,
      ast,
      format,
      system,
      customQueryID,
      currentJunction,
      (tableName, ast, format, customQueryID, currentJunction) =>
        new RunnableQueryImpl(
          delegate,
          schema,
          tableName,
          ast,
          format,
          system,
          customQueryID,
          currentJunction,
        ),
    );
    this.#delegate = delegate;
  }

  run(options?: RunOptions): Promise<HumanReadable<TReturn>> {
    return this.#delegate.run(this, options);
  }

  preload(options?: PreloadOptions): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    return this.#delegate.preload(this, options);
  }

  materialize(ttl?: TTL): TypedView<HumanReadable<TReturn>>;
  materialize<T>(
    factory: ViewFactory<TTable, TSchema, TReturn, T>,
    ttl?: TTL,
  ): T;
  materialize<T>(
    factory?: unknown,
    ttl?: unknown,
  ): T | TypedView<HumanReadable<TReturn>> {
    let actualFactory: ViewFactory<TTable, TSchema, TReturn, T> | undefined;
    let options: MaterializeOptions | undefined;

    if (typeof factory === 'function') {
      actualFactory = factory as ViewFactory<TTable, TSchema, TReturn, T>;
      options = {ttl: ttl as TTL | undefined};
    } else {
      actualFactory = undefined;
      options = {ttl: factory as TTL | undefined};
    }

    return this.#delegate.materialize(this, actualFactory, options);
  }
}
