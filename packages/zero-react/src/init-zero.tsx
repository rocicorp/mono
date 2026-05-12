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
  type ZeroProviderProps,
} from './zero-provider.tsx';
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

export type WrapZeroReactResult<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TWrappedTransaction,
  TSchemaBound extends boolean = false,
> = InitZeroResult<S, C, TWrappedTransaction, TSchemaBound> & {
  readonly ZeroProvider: TypedZeroProvider<S, C, TSchemaBound>;
  readonly useZero: typeof useZeroImpl<S, undefined, C>;
  readonly useQuery: TypedUseQuery<S, C>;
  readonly useSuspenseQuery: TypedUseSuspenseQuery<S, C>;
  readonly useConnectionState: typeof useConnectionStateImpl;
};

/**
 * Adds typed React hooks and provider to typed Zero helpers.
 */
export function wrapZeroReact<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TWrappedTransaction,
  TSchemaBound extends boolean,
>(
  zero: InitZeroResult<S, C, TWrappedTransaction, TSchemaBound>,
): WrapZeroReactResult<S, C, TWrappedTransaction, TSchemaBound> {
  const ZeroProvider =
    'schema' in zero
      ? (props: WithoutSchema<ZeroProviderProps<S, undefined, C>>) => {
          if ('zero' in props) {
            return (
              <ZeroProviderImpl
                {...(props as ZeroProviderProps<S, undefined, C>)}
              />
            );
          }

          const typedProps = {
            ...(props as object),
            schema: (zero as {readonly schema: S}).schema,
          } as ZeroProviderProps<S, undefined, C>;
          return <ZeroProviderImpl {...typedProps} />;
        }
      : ZeroProviderImpl;

  return {
    ...zero,
    ZeroProvider: ZeroProvider as TypedZeroProvider<S, C, boolean>,
    useZero: useZeroImpl as typeof useZeroImpl<S, undefined, C>,
    useQuery: useQueryImpl as TypedUseQuery<S, C>,
    useSuspenseQuery: useSuspenseQueryImpl as TypedUseSuspenseQuery<S, C>,
    useConnectionState: useConnectionStateImpl,
  } as WrapZeroReactResult<S, C, TWrappedTransaction, TSchemaBound>;
}
