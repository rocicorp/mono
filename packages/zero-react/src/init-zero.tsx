import {useConnectionState as useConnectionStateImpl} from './use-connection-state.tsx';
import {
  useQuery as useQueryImpl,
  useSuspenseQuery as useSuspenseQueryImpl,
  type TypedUseQuery,
  type TypedUseSuspenseQuery,
} from './use-query.tsx';
import {
  useZero as useZeroImpl,
  ZeroProvider as ZeroProviderImpl,
} from './zero-provider.tsx';
import type {
  BaseDefaultContext,
  BaseDefaultSchema,
  DefaultContext,
  DefaultSchema,
  TypedZero,
} from './zero.ts';
import {Zero as ZeroImpl} from './zero.ts';

export type InitZeroReactResult<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
> = {
  readonly Zero: TypedZero<S, C>;
  readonly ZeroProvider: typeof ZeroProviderImpl<S, undefined, C>;
  readonly useZero: typeof useZeroImpl<S, undefined, C>;
  readonly useQuery: TypedUseQuery<S, C>;
  readonly useSuspenseQuery: TypedUseSuspenseQuery<S, C>;
  readonly useConnectionState: typeof useConnectionStateImpl;
};

/**
 * Returns typed React hooks and provider without relying on module augmentation.
 */
export function initZeroReact<
  S extends BaseDefaultSchema = DefaultSchema,
  C extends BaseDefaultContext = DefaultContext,
>(): InitZeroReactResult<S, C> {
  return {
    Zero: ZeroImpl as TypedZero<S, C>,
    ZeroProvider: ZeroProviderImpl as typeof ZeroProviderImpl<S, undefined, C>,
    useZero: useZeroImpl as typeof useZeroImpl<S, undefined, C>,
    useQuery: useQueryImpl as TypedUseQuery<S, C>,
    useSuspenseQuery: useSuspenseQueryImpl as TypedUseSuspenseQuery<S, C>,
    useConnectionState: useConnectionStateImpl,
  };
}
