import {
  createEffect,
  createMemo,
  createSignal,
  on,
  onCleanup,
  untrack,
  type Accessor,
} from 'solid-js';
import {createStore} from 'solid-js/store';
import {bindingsForZero} from '../../zero-client/src/client/bindings.ts';
import type {QueryResultDetails} from '../../zero-client/src/types/query-result.ts';
import type {
  DefaultContext,
  DefaultSchema,
} from '../../zero-types/src/default-types.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {
  HumanReadable,
  PullRow,
  ToZQL,
} from '../../zql/src/query/query-builder.ts';
import {DEFAULT_TTL_MS, type TTL} from '../../zql/src/query/ttl.ts';
import {createSolidViewFactory, UNKNOWN, type State} from './solid-view.ts';
import {useZero} from './use-zero.ts';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails & {}>,
];

// Deprecated in 0.22
/**
 * @deprecated Use {@linkcode UseQueryOptions} instead.
 */
export type CreateQueryOptions = {
  ttl?: TTL | undefined;
};

export type UseQueryOptions = {
  ttl?: TTL | undefined;
};

// Deprecated in 0.22
/**
 * @deprecated Use {@linkcode useQuery} instead.
 */
export function createQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  querySignal: Accessor<ToZQL<TTable, TSchema, TReturn, TContext>>,
  options?: CreateQueryOptions | Accessor<CreateQueryOptions>,
): QueryResult<TReturn> {
  return useQuery(querySignal, options);
}

export function useQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  querySignal: Accessor<ToZQL<TTable, TSchema, TReturn, TContext>>,
  options?: UseQueryOptions | Accessor<UseQueryOptions>,
): QueryResult<TReturn> {
  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    UNKNOWN,
  ]);
  const initialRefetchKey = 0;
  const [refetchKey, setRefetchKey] = createSignal(initialRefetchKey);

  const refetch = () => {
    setRefetchKey(k => k + 1);
  };

  const zero = useZero<TSchema, undefined, TContext>();

  const query = createMemo(() => querySignal().toZQL(zero().context));
  const bindings = createMemo(() => bindingsForZero(zero()));
  const hash = createMemo(() => bindings().hash(query()));
  const ttl = createMemo(() => normalize(options)?.ttl ?? DEFAULT_TTL_MS);

  const initialTTL = ttl();

  const view = createMemo(() => {
    // Depend on hash instead of query to avoid recreating the view when the
    // query object changes but the hash is the same.
    hash();
    refetchKey();
    const b = bindings();
    const q = untrack(query);

    const v = b.materialize(q, createSolidViewFactory(setState, refetch), {
      ttl: initialTTL,
    });

    onCleanup(() => v.destroy());

    return v;
  });

  // Update TTL on existing view when it changes.
  createEffect(
    on(
      ttl,
      currentTTL => {
        view().updateTTL(currentTTL);
      },
      {defer: true},
    ),
  );

  return [() => state[0][''] as HumanReadable<TReturn>, () => state[1]];
}

function normalize<T>(options?: T | Accessor<T | undefined>): T | undefined {
  return typeof options === 'function' ? (options as Accessor<T>)() : options;
}
