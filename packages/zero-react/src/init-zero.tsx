import {
  useQuery as useQueryImpl,
  useSuspenseQuery as useSuspenseQueryImpl,
  type TypedUseQuery,
  type TypedUseSuspenseQuery,
} from './use-query.tsx';
import {
  useZero as useZeroImpl,
  ZeroProvider as ZeroProviderImpl,
  type ZeroProviderProps,
} from './zero-provider.tsx';
import type {Schema, Zero} from './zero.ts';
import {useConnectionState as useConnectionStateImpl} from './use-connection-state.tsx';

/**
 * The result of calling initZero for React.
 */
export type InitZeroReactResult<S extends Schema, C> = {
  /**
   * React provider component for the Zero instance.
   * Pass either ZeroOptions props or a pre-created Zero instance.
   */
  readonly ZeroProvider: (
    props: ZeroProviderProps<S, undefined, C>,
  ) => ReturnType<typeof ZeroProviderImpl>;

  /**
   * Hook to get the Zero client instance from context.
   * Must be used within a ZeroProvider.
   */
  readonly useZero: () => Zero<S, undefined, C>;

  /**
   * Hook to subscribe to query results.
   * Returns [data, resultDetails] tuple.
   */
  readonly useQuery: TypedUseQuery<S, C>;

  /**
   * Hook to subscribe to query results with Suspense support.
   * Suspends until results are available.
   */
  readonly useSuspenseQuery: TypedUseSuspenseQuery<S, C>;

  /**
   * Hook to subscribe to the connection state of the Zero instance.
   * Returns the connection state.
   */
  readonly useConnectionState: typeof useConnectionStateImpl;
};

/**
 * Initialize Zero for React with a schema and return typed hooks and provider.
 *
 * This provides typed versions of the existing Zero React hooks. The types
 * flow from the schema passed to initZero through all returned utilities.
 *
 * @example
 * ```tsx
 * // src/zero.ts
 * import {initZero} from '@rocicorp/zero/react';
 * import type {Schema} from '../shared/schema';
 *
 * type AuthData = {userID: string; role: 'admin' | 'user'};
 *
 * export const {
 *   ZeroProvider,
 *   useZero,
 *   useQuery,
 *   useSuspenseQuery,
 *   useConnectionState,
 * } = initZero<Schema, AuthData | undefined>();
 * ```
 *
 * @param _options - Configuration options including the schema (used for type inference only)
 * @returns Typed React hooks and provider
 */
export function initZero<
  S extends Schema = Schema,
  C = unknown,
>(): InitZeroReactResult<S, C> {
  return {
    ZeroProvider: ZeroProviderImpl as (
      props: ZeroProviderProps<S, undefined, C>,
    ) => ReturnType<typeof ZeroProviderImpl>,
    useZero: useZeroImpl as () => Zero<S, undefined, C>,
    useQuery: useQueryImpl as TypedUseQuery<S, C>,
    useSuspenseQuery: useSuspenseQueryImpl as TypedUseSuspenseQuery<S, C>,
    useConnectionState: useConnectionStateImpl,
  };
}
