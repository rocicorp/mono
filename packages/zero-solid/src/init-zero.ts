import {useConnectionState as useConnectionStateImpl} from './use-connection-state.ts';
import {useQuery as useQueryImpl, type TypedUseQuery} from './use-query.ts';
import {
  useZero as useZeroImpl,
  ZeroProvider as ZeroProviderImpl,
} from './use-zero.ts';
import type {
  BaseDefaultContext,
  BaseDefaultSchema,
  DefaultContext,
  DefaultSchema,
  TypedZero,
} from './zero.ts';
import {Zero as ZeroImpl} from './zero.ts';

export type InitZeroSolidResult<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
> = {
  readonly Zero: TypedZero<S, C>;
  readonly ZeroProvider: typeof ZeroProviderImpl<S, undefined, C>;
  readonly useZero: typeof useZeroImpl<S, undefined, C>;
  readonly useQuery: TypedUseQuery<S, C>;
  readonly useConnectionState: typeof useConnectionStateImpl;
};

/**
 * Returns typed Solid hooks and provider without relying on module augmentation.
 */
export function initZeroSolid<
  S extends BaseDefaultSchema = DefaultSchema,
  C extends BaseDefaultContext = DefaultContext,
>(): InitZeroSolidResult<S, C> {
  return {
    Zero: ZeroImpl as TypedZero<S, C>,
    ZeroProvider: ZeroProviderImpl as typeof ZeroProviderImpl<S, undefined, C>,
    useZero: useZeroImpl as typeof useZeroImpl<S, undefined, C>,
    useQuery: useQueryImpl as TypedUseQuery<S, C>,
    useConnectionState: useConnectionStateImpl,
  };
}
