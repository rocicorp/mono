import type {Accessor, JSX} from 'solid-js';
import {
  useQuery as useQueryImpl,
  type MaybeQueryResult,
  type QueryResult,
  type TypedUseQuery,
  type UseQueryOptions,
} from './use-query.ts';
import {
  useZero as useZeroImpl,
  ZeroProvider as ZeroProviderImpl,
  type ZeroProviderProps,
} from './use-zero.ts';
import type {Schema, Zero} from './zero.ts';
import {useConnectionState as useConnectionStateImpl} from './use-connection-state.ts';

export type {MaybeQueryResult, QueryResult, UseQueryOptions};

/**
 * The result of calling initZero for Solid.
 */
export type InitZeroSolidResult<S extends Schema, C> = {
  /**
   * Solid provider component for the Zero instance.
   * Pass either ZeroOptions props or a pre-created Zero instance.
   */
  readonly ZeroProvider: (
    props: ZeroProviderProps<S, undefined, C>,
  ) => ReturnType<typeof ZeroProviderImpl>;

  /**
   * Hook to get the Zero client accessor from context.
   * Returns an Accessor (function) that returns the Zero instance.
   * Must be used within a ZeroProvider.
   */
  readonly useZero: () => Accessor<Zero<S, undefined, C>>;

  /**
   * Hook to subscribe to query results.
   * Returns [dataAccessor, resultDetailsAccessor] tuple.
   */
  readonly useQuery: TypedUseQuery<S, C>;

  /**
   * Hook to subscribe to the connection state of the Zero instance.
   * Returns an Accessor for the connection state.
   */
  readonly useConnectionState: typeof useConnectionStateImpl;
};

/**
 * Initialize Zero for Solid with a schema and return typed hooks and provider.
 *
 * This provides typed versions of the existing Zero Solid hooks. The types
 * flow from the schema passed to initZero through all returned utilities.
 *
 * @example
 * ```tsx
 * // src/zero.ts
 * import {initZero} from '@rocicorp/zero/solid';
 * import type {Schema} from '../shared/schema';
 *
 * type AuthData = {userID: string; role: 'admin' | 'user'};
 *
 * export const {
 *   ZeroProvider,
 *   useZero,
 *   useQuery,
 *   useConnectionState,
 * } = initZero<Schema, AuthData | undefined>();
 * ```
 *
 * @param _options - Configuration options including the schema (used for type inference only)
 * @returns Typed Solid hooks and provider
 */
export function initZero<
  S extends Schema = Schema,
  C = unknown,
>(): InitZeroSolidResult<S, C> {
  return {
    ZeroProvider: ZeroProviderImpl as (
      props: ZeroProviderProps<S, undefined, C>,
    ) => JSX.Element,
    useZero: useZeroImpl as () => Accessor<Zero<S, undefined, C>>,
    useQuery: useQueryImpl as TypedUseQuery<S, C>,
    useConnectionState: useConnectionStateImpl,
  };
}
