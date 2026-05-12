import {mergeProps} from 'solid-js';
import {useConnectionState as useConnectionStateImpl} from './use-connection-state.ts';
import {useQuery as useQueryImpl, type TypedUseQuery} from './use-query.ts';
import {
  useZero as useZeroImpl,
  ZeroProvider as ZeroProviderImpl,
  type ZeroProviderProps,
} from './use-zero.ts';
import type {
  BaseDefaultContext,
  BaseDefaultSchema,
  InitZeroResult,
} from './zero.ts';

type WithoutSchema<T> = T extends unknown ? Omit<T, 'schema'> : never;

type TypedZeroProvider<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TSchemaBound extends boolean,
> = (
  props: TSchemaBound extends true
    ? WithoutSchema<ZeroProviderProps<S, undefined, C>>
    : ZeroProviderProps<S, undefined, C>,
) => ReturnType<typeof ZeroProviderImpl<S, undefined, C>>;

export type WrapZeroSolidResult<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TWrappedTransaction,
  TSchemaBound extends boolean = false,
> = InitZeroResult<S, C, TWrappedTransaction, TSchemaBound> & {
  readonly ZeroProvider: TypedZeroProvider<S, C, TSchemaBound>;
  readonly useZero: typeof useZeroImpl<S, undefined, C>;
  readonly useQuery: TypedUseQuery<S, C>;
  readonly useConnectionState: typeof useConnectionStateImpl;
};

/**
 * Adds typed Solid hooks and provider to typed Zero helpers.
 */
export function wrapZeroSolid<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TWrappedTransaction,
  TSchemaBound extends boolean,
>(
  zero: InitZeroResult<S, C, TWrappedTransaction, TSchemaBound>,
): WrapZeroSolidResult<S, C, TWrappedTransaction, TSchemaBound> {
  const ZeroProvider =
    'schema' in zero
      ? (props: WithoutSchema<ZeroProviderProps<S, undefined, C>>) => {
          if ('zero' in props) {
            return ZeroProviderImpl(
              props as ZeroProviderProps<S, undefined, C>,
            );
          }

          return ZeroProviderImpl(
            mergeProps(props, {
              schema: (zero as {readonly schema: S}).schema,
            }) as ZeroProviderProps<S, undefined, C>,
          );
        }
      : ZeroProviderImpl;

  return {
    ...zero,
    ZeroProvider: ZeroProvider as TypedZeroProvider<S, C, boolean>,
    useZero: useZeroImpl as typeof useZeroImpl<S, undefined, C>,
    useQuery: useQueryImpl as TypedUseQuery<S, C>,
    useConnectionState: useConnectionStateImpl,
  } as WrapZeroSolidResult<S, C, TWrappedTransaction, TSchemaBound>;
}
