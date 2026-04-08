import {
  batch,
  createContext,
  createEffect,
  createMemo,
  onCleanup,
  splitProps,
  untrack,
  useContext,
  type Accessor,
  type JSX,
} from 'solid-js';
import {
  Zero,
  type BaseDefaultContext,
  type BaseDefaultSchema,
  type CustomMutatorDefs,
  type DefaultContext,
  type DefaultSchema,
  type ZeroOptions,
} from './zero.ts';

const ZeroContext = createContext<
  // oxlint-disable-next-line no-explicit-any
  Accessor<Zero<any, any, any>> | undefined
>(undefined);

/**
 * @deprecated Use {@linkcode ZeroProvider} instead of managing your own Zero instance.
 */
export function createZero<
  S extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context extends BaseDefaultContext = DefaultContext,
>(options: ZeroOptions<S, MD, Context>): Zero<S, MD, Context> {
  const opts = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero<S, MD, Context>(opts);
}

export function useZero<
  S extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context extends BaseDefaultContext = DefaultContext,
>(): () => Zero<S, MD, Context> {
  const zero = useContext(ZeroContext);

  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero;
}

/**
 * @deprecated Use {@linkcode useZero} instead, alongside default types defined with:
 *
 * ```ts
 * declare module '@rocicorp/zero' {
 *   interface DefaultTypes {
 *     schema: typeof schema;
 *     context: Context;
 *   }
 * }
 */
export function createUseZero<
  S extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context extends BaseDefaultContext = DefaultContext,
>() {
  return () => useZero<S, MD, Context>();
}

export function ZeroProvider<
  S extends BaseDefaultSchema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context extends BaseDefaultContext = DefaultContext,
>(
  props: {
    children: JSX.Element;
    init?: (zero: Zero<S, MD, Context>) => void;
  } & (
    | {
        zero: Zero<S, MD, Context>;
      }
    | ZeroOptions<S, MD, Context>
  ),
) {
  let prevAuth = 'auth' in props ? props.auth : undefined;

  const auth = createMemo(() => ('auth' in props ? props.auth : undefined));
  const hasAuth = createMemo(() => typeof auth() === 'string');

  const zero = createMemo(() => {
    if ('zero' in props) {
      return props.zero;
    }

    hasAuth();

    const [, options] = splitProps(props, ['children', 'auth']);

    const authValue = untrack(auth);
    prevAuth = authValue;
    const createdZero = new Zero({
      ...options,
      ...(authValue !== undefined ? {auth: authValue} : {}),
      batchViewUpdates: batch,
    });
    options.init?.(createdZero);
    onCleanup(() => createdZero.close());
    return createdZero;
  });

  createEffect(() => {
    const currentZero = zero();
    if (!currentZero || 'zero' in props) {
      return;
    }

    const currentAuth = auth();

    if (currentAuth !== prevAuth) {
      const previousAuth = prevAuth;
      prevAuth = currentAuth;

      if (typeof previousAuth === 'string' && typeof currentAuth === 'string') {
        void currentZero.connection.connect({auth: currentAuth});
      }
    }
  });

  return ZeroContext.Provider({
    value: zero,
    get children() {
      return props.children;
    },
  });
}
