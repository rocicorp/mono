import {
  asQueryInternals,
  deepClone,
  type Immutable,
} from '../../zero-client/src/client/bindings.ts';
import {Query as SvelteQuery, type QueryOptions} from './query.svelte.ts';
import {
  Zero,
  type BaseDefaultContext,
  type BaseDefaultSchema,
  type Connection,
  type ConnectionState,
  type CustomMutatorDefs,
  type DefaultContext,
  type DefaultSchema,
  type ErroredQuery,
  type Falsy,
  type HumanReadable,
  type MaterializeOptions,
  type PullRow,
  type Query as QueryDef,
  type QueryErrorDetails,
  type QueryOrQueryRequest,
  type QueryResultDetails,
  type ReadonlyJSONValue,
  type ResultType,
  type RunOptions,
  type TTL,
  type TypedView,
  type ZeroOptions,
} from './zero-client.ts';

export type QueryResult<TReturn> = readonly [
  HumanReadable<TReturn> | undefined,
  QueryResultDetails,
];

const UNKNOWN: QueryResultDetails = Object.freeze({type: 'unknown'});
const COMPLETE: QueryResultDetails = Object.freeze({type: 'complete'});
const EMPTY_ARRAY: readonly [] = Object.freeze([]);

type ZLike<
  TSchema extends BaseDefaultSchema,
  TContext extends BaseDefaultContext,
> = {
  readonly clientID: string;
  readonly context: TContext;
  materialize<
    TTable extends keyof TSchema['tables'] & string,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TReturn,
  >(
    query: QueryOrQueryRequest<
      TTable,
      TInput,
      TOutput,
      TSchema,
      TReturn,
      TContext
    >,
    options?: MaterializeOptions | undefined,
  ): TypedView<HumanReadable<TReturn>>;
};

export class ViewStore {
  #views = new Map<string, object>();

  getView<
    TSchema extends BaseDefaultSchema,
    TReturn,
    TContext extends BaseDefaultContext,
  >(
    z: ZLike<TSchema, TContext>,
    query:
      | QueryDef<keyof TSchema['tables'] & string, TSchema, TReturn>
      | undefined,
    enabled: boolean,
    ttl: TTL,
  ): ViewWrapper<TSchema, TReturn, TContext> {
    if (!query || !enabled) {
      return new ViewWrapper(z, query, enabled, ttl, () => {});
    }

    const qi = asQueryInternals(query);
    const hash = qi.hash() + (qi.format.singular ? 't' : 'f') + z.clientID;
    const existing = this.#views.get(hash) as
      | ViewWrapper<TSchema, TReturn, TContext>
      | undefined;
    if (existing) {
      existing.updateTTL(ttl);
      return existing;
    }

    const view = new ViewWrapper(z, query, enabled, ttl, () => {
      if (this.#views.get(hash) === view) {
        this.#views.delete(hash);
      }
    });
    this.#views.set(hash, view);
    return view;
  }
}

export class ViewWrapper<
  TSchema extends BaseDefaultSchema,
  TReturn,
  TContext extends BaseDefaultContext,
> {
  #view: TypedView<HumanReadable<TReturn>> | undefined;
  #unsubscribe: (() => void) | undefined;
  #ttl: TTL;
  #snapshot = $state.raw<QueryResult<TReturn>>([undefined, UNKNOWN]);
  readonly #singular: boolean | undefined;
  readonly #z: ZLike<TSchema, TContext>;
  readonly #query:
    | QueryDef<keyof TSchema['tables'] & string, TSchema, TReturn>
    | undefined;
  readonly #enabled: boolean;
  readonly #onDestroy: () => void;

  constructor(
    z: ZLike<TSchema, TContext>,
    query:
      | QueryDef<keyof TSchema['tables'] & string, TSchema, TReturn>
      | undefined,
    enabled: boolean,
    ttl: TTL,
    onDestroy: () => void,
  ) {
    this.#z = z;
    this.#query = query;
    this.#enabled = enabled;
    this.#onDestroy = onDestroy;
    this.#ttl = ttl;
    this.#singular = query
      ? asQueryInternals(query).format.singular
      : undefined;
    this.#snapshot = [this.#emptyData(), UNKNOWN];
    this.#materialize();
  }

  get data(): HumanReadable<TReturn> | undefined {
    return this.#snapshot[0];
  }

  get details(): QueryResultDetails {
    return this.#snapshot[1];
  }

  get result(): QueryResult<TReturn> {
    return this.#snapshot;
  }

  updateTTL(ttl: TTL): void {
    this.#ttl = ttl;
    this.#view?.updateTTL(ttl);
  }

  destroy(): void {
    this.#resetView();
    this.#onDestroy();
  }

  #emptyData(): HumanReadable<TReturn> | undefined {
    if (this.#singular === false) {
      return EMPTY_ARRAY as unknown as HumanReadable<TReturn>;
    }
    return undefined;
  }

  #materialize(): void {
    if (!this.#query || !this.#enabled || this.#view) {
      return;
    }

    this.#view = this.#z.materialize(this.#query, {ttl: this.#ttl});
    this.#unsubscribe = this.#view.addListener((data, type, error) => {
      this.#snapshot = [this.#clone(data), this.#details(type, error)];
    });
  }

  #resetView(): void {
    this.#unsubscribe?.();
    this.#unsubscribe = undefined;
    this.#view?.destroy();
    this.#view = undefined;
    this.#snapshot = [this.#emptyData(), UNKNOWN];
  }

  #retry = (): void => {
    this.#resetView();
    this.#materialize();
  };

  #clone(
    data: Immutable<HumanReadable<TReturn>>,
  ): HumanReadable<TReturn> | undefined {
    if (data === undefined) {
      return undefined;
    }
    return deepClone(data as ReadonlyJSONValue) as HumanReadable<TReturn>;
  }

  #details(type: ResultType, error?: ErroredQuery): QueryResultDetails {
    if (type === 'complete') {
      return COMPLETE;
    }
    if (type === 'error') {
      return this.#error(error);
    }
    return UNKNOWN;
  }

  #error(error?: ErroredQuery): QueryErrorDetails {
    const message = error?.message ?? 'An unknown error occurred';
    return {
      type: 'error',
      retry: this.#retry,
      refetch: this.#retry,
      error: {
        type: error?.error ?? 'app',
        message,
        ...(error?.details ? {details: error.details} : {}),
      },
    };
  }
}

export class Z<
  TSchema extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  TContext extends BaseDefaultContext = DefaultContext,
> {
  #zero: Zero<TSchema, MD, TContext>;
  #connectionUnsubscribe: (() => void) | undefined;
  #connectionState = $state<ConnectionState>({name: 'connecting'});
  readonly viewStore = new ViewStore();

  constructor(options: ZeroOptions<TSchema, MD, TContext>) {
    this.#zero = this.#createZero(options);
  }

  get query(): Zero<TSchema, MD, TContext>['query'] {
    return this.#zero.query;
  }

  get mutate(): Zero<TSchema, MD, TContext>['mutate'] {
    return this.#zero.mutate;
  }

  get mutateBatch(): Zero<TSchema, MD, TContext>['mutateBatch'] {
    return this.#zero.mutateBatch;
  }

  get clientID(): string {
    return this.#zero.clientID;
  }

  get userID(): string | undefined {
    return this.#zero.userID;
  }

  get context(): TContext {
    return this.#zero.context;
  }

  get connection(): Connection {
    return this.#zero.connection;
  }

  get connectionState(): ConnectionState {
    return this.#connectionState;
  }

  get online(): boolean {
    return this.#connectionState.name === 'connected';
  }

  createQuery<
    TTable extends keyof TSchema['tables'] & string,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TReturn = PullRow<TTable, TSchema>,
  >(
    query:
      | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
      | Falsy,
    options?: QueryOptions | boolean,
  ): SvelteQuery<TSchema, TReturn, TContext, MD, TTable, TInput, TOutput> {
    return new SvelteQuery(this, query, options);
  }

  q<
    TTable extends keyof TSchema['tables'] & string,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TReturn = PullRow<TTable, TSchema>,
  >(
    query:
      | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
      | Falsy,
    options?: QueryOptions | boolean,
  ): SvelteQuery<TSchema, TReturn, TContext, MD, TTable, TInput, TOutput> {
    return this.createQuery(query, options);
  }

  preload<
    TTable extends keyof TSchema['tables'] & string,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TReturn = PullRow<TTable, TSchema>,
  >(
    query: QueryOrQueryRequest<
      TTable,
      TInput,
      TOutput,
      TSchema,
      TReturn,
      TContext
    >,
    options?: {ttl?: TTL | undefined} | undefined,
  ): {cleanup: () => void; complete: Promise<void>} {
    return this.#zero.preload(query, options);
  }

  run<
    TTable extends keyof TSchema['tables'] & string,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TReturn = PullRow<TTable, TSchema>,
  >(
    query: QueryOrQueryRequest<
      TTable,
      TInput,
      TOutput,
      TSchema,
      TReturn,
      TContext
    >,
    options?: RunOptions | undefined,
  ): Promise<HumanReadable<TReturn>> {
    return this.#zero.run(query, options);
  }

  materialize<
    TTable extends keyof TSchema['tables'] & string,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TReturn,
  >(
    query: QueryOrQueryRequest<
      TTable,
      TInput,
      TOutput,
      TSchema,
      TReturn,
      TContext
    >,
    options?: MaterializeOptions | undefined,
  ): TypedView<HumanReadable<TReturn>> {
    return this.#zero.materialize(query, options);
  }

  build(options: ZeroOptions<TSchema, MD, TContext>): void {
    this.#connectionUnsubscribe?.();
    this.#connectionUnsubscribe = undefined;
    void this.#zero.close();
    this.#zero = this.#createZero(options);
  }

  close(): void {
    this.#connectionUnsubscribe?.();
    this.#connectionUnsubscribe = undefined;
    void this.#zero.close();
  }

  #createZero(options: ZeroOptions<TSchema, MD, TContext>) {
    const z = new Zero(options);
    this.#connectionState = z.connection.state.current;
    this.#connectionUnsubscribe = z.connection.state.subscribe(state => {
      this.#connectionState = state;
    });
    return z;
  }
}
