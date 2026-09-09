import {
  tableAST,
  type NormalizedAST,
  type System,
} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import type {CustomQueryID} from './named.ts';
import type {QueryDelegate} from './query-delegate.ts';
import {QueryImpl} from './query-impl.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  PullRow,
  Query,
  RunOptions,
} from './query.ts';
import type {TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

/**
 * Runnable roots are interned per delegate *and* schema -- a query is only
 * interchangeable with another if it would run against the same delegate. See
 * `query-transitions.ts` for why roots need to be shared at all.
 */
const rootsByDelegate = new WeakMap<
  QueryDelegate,
  // oxlint-disable-next-line no-explicit-any
  WeakMap<Schema, Map<string, WeakRef<RunnableQueryImpl<any, any, any>>>>
>();

export function newRunnableQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
>(
  delegate: QueryDelegate,
  schema: TSchema,
  table: TTable,
): Query<TTable, TSchema> {
  let bySchema = rootsByDelegate.get(delegate);
  if (!bySchema) {
    bySchema = new WeakMap();
    rootsByDelegate.set(delegate, bySchema);
  }
  let roots = bySchema.get(schema);
  if (!roots) {
    roots = new Map();
    bySchema.set(schema, roots);
  }

  const existing = roots.get(table)?.deref();
  if (existing) {
    return existing as RunnableQueryImpl<TTable, TSchema>;
  }

  const created = new RunnableQueryImpl<TTable, TSchema>(
    delegate,
    schema,
    table,
    tableAST(table),
    defaultFormat,
    undefined,
  );
  roots.set(table, new WeakRef(created));
  return created;
}

export class RunnableQueryImpl<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
>
  extends QueryImpl<TTable, TSchema, TReturn>
  implements Query<TTable, TSchema, TReturn>
{
  readonly #delegate: QueryDelegate;

  constructor(
    delegate: QueryDelegate,
    schema: TSchema,
    tableName: TTable,
    ast: NormalizedAST = tableAST(tableName),
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

  override run(options?: RunOptions): Promise<HumanReadable<TReturn>> {
    // The type arguments on the delegate calls in this class are written out
    // because tsc 7.0.2 fails to infer them in some program layouts (the
    // packages/zero declaration build, apps/zero-throughput): inference
    // collapses TSchema to its constraint and the call misreports as a
    // TS2345 assignability error. Explicit arguments are inert where
    // inference works and correct where it does not.
    return this.#delegate.run<TTable, TSchema, TReturn>(this, options);
  }

  override preload(options?: PreloadOptions): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    return this.#delegate.preload<TTable, TSchema, TReturn>(this, options);
  }

  override materialize(ttl?: TTL): TypedView<HumanReadable<TReturn>>;
  override materialize<T>(
    factory: ViewFactory<TTable, TSchema, TReturn, T>,
    ttl?: TTL,
  ): T;
  override materialize<T>(
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

    return this.#delegate.materialize<TTable, TSchema, TReturn, T>(
      this,
      actualFactory,
      options,
    );
  }
}
