import {
  DEFAULT_TTL_MS,
  addContextToQuery,
} from '../../zero-client/src/client/bindings.ts';
import type {
  BaseDefaultContext,
  BaseDefaultSchema,
  CustomMutatorDefs,
  Falsy,
  HumanReadable,
  Query as QueryDef,
  QueryOrQueryRequest,
  QueryResultDetails,
  ReadonlyJSONValue,
  TTL,
} from './zero-client.ts';
import type {QueryResult, ViewWrapper, Z} from './zero.svelte.ts';

export type {QueryResultDetails};

export type QueryOptions = {
  enabled?: boolean | undefined;
  ttl?: TTL | undefined;
};

export class Query<
  TSchema extends BaseDefaultSchema,
  TReturn,
  TContext extends BaseDefaultContext,
  MD extends CustomMutatorDefs | undefined = undefined,
  TTable extends keyof TSchema['tables'] & string = keyof TSchema['tables'] &
    string,
  TInput extends ReadonlyJSONValue | undefined = ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined = ReadonlyJSONValue | undefined,
> {
  #view = $state.raw<ViewWrapper<TSchema, TReturn, TContext> | undefined>(
    undefined,
  );
  readonly #z: Z<TSchema, MD, TContext>;

  constructor(
    z: Z<TSchema, MD, TContext>,
    query:
      | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
      | Falsy,
    options?: QueryOptions | boolean,
  ) {
    this.#z = z;
    this.updateQuery(query, options);
  }

  get data(): HumanReadable<TReturn> | undefined {
    return this.#view?.data;
  }

  get details(): QueryResultDetails {
    return this.#view?.details ?? {type: 'unknown'};
  }

  get current(): HumanReadable<TReturn> | undefined {
    return this.data;
  }

  get result(): QueryResult<TReturn> {
    return this.#view?.result ?? [undefined, {type: 'unknown'}];
  }

  updateQuery(
    query:
      | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
      | Falsy,
    options?: QueryOptions | boolean,
  ): void {
    const q = resolveQuery(this.#z, query);
    const opts = normalizeOptions(options);
    this.#view = this.#z.viewStore.getView(this.#z, q, opts.enabled, opts.ttl);
  }

  destroy(): void {
    this.#view?.destroy();
    this.#view = undefined;
  }
}

function resolveQuery<
  TSchema extends BaseDefaultSchema,
  MD extends CustomMutatorDefs | undefined,
  TReturn,
  TContext extends BaseDefaultContext,
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  z: Z<TSchema, MD, TContext>,
  query:
    | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
    | Falsy,
): QueryDef<TTable, TSchema, TReturn> | undefined {
  if (!query) {
    return undefined;
  }
  return addContextToQuery(query, z.context);
}

function normalizeOptions(options?: QueryOptions | boolean): {
  enabled: boolean;
  ttl: TTL;
} {
  if (typeof options === 'boolean') {
    return {enabled: options, ttl: DEFAULT_TTL_MS};
  }
  return {
    enabled: options?.enabled ?? true,
    ttl: options?.ttl ?? DEFAULT_TTL_MS,
  };
}
