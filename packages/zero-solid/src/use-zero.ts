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
  type CustomMutatorDefs,
  type DefaultContext,
  type DefaultSchema,
  type Schema,
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
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
>(options: ZeroOptions<S, MD, Context>): Zero<S, MD, Context> {
  const opts = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero(opts);
}

export function useZero<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
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
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
>() {
  return () => useZero<S, MD, Context>();
}

const NO_AUTH_SET = Symbol();

export type ZeroProviderProps<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
> = {
  children: JSX.Element;
  init?: (zero: Zero<S, MD, Context>) => void;
} & (
  | {
      zero: Zero<S, MD, Context>;
    }
  | ZeroOptions<S, MD, Context>
);

export function ZeroProvider<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
>(props: ZeroProviderProps<S, MD, Context>) {
  const zero = createMemo(() => {
    if ('zero' in props) {
      return props.zero;
    }

    const [, options] = splitProps(props, ['children', 'auth']);

    const authValue = untrack(() => props.auth);
    const createdZero = new Zero({
      ...options,
      ...(authValue !== undefined ? {auth: authValue} : {}),
      batchViewUpdates: batch,
    });
    options.init?.(createdZero);
    onCleanup(() => createdZero.close());
    return createdZero;
  });

  const auth = createMemo<
    typeof NO_AUTH_SET | ZeroOptions<S, MD, Context>['auth']
  >(() => ('auth' in props ? props.auth : NO_AUTH_SET));

  let prevAuth: typeof NO_AUTH_SET | ZeroOptions<S, MD, Context>['auth'] =
    NO_AUTH_SET;

  createEffect(() => {
    const currentZero = zero();
    if (!currentZero || 'zero' in props) {
      return;
    }

    const currentAuth = auth();

    if (prevAuth === NO_AUTH_SET) {
      prevAuth = currentAuth;
      return;
    }

    if (currentAuth !== prevAuth) {
      prevAuth = currentAuth;
      void currentZero.connection.connect({
        auth: currentAuth === NO_AUTH_SET ? undefined : currentAuth,
      });
    }
  });

  return ZeroContext.Provider({
    value: zero,
    get children() {
      return props.children;
    },
  });
}
