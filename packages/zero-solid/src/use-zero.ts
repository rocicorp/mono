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
import {isJSONEqual} from '../../shared/src/json.ts';
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

const stableMemoOptions = {
  equals: isJSONEqual,
};

export function ZeroProvider<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
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
  const context = createMemo(
    () => ('zero' in props ? undefined : props.context),
    undefined,
    stableMemoOptions,
  );
  const mutateHeaders = createMemo(
    () => ('zero' in props ? undefined : props.mutateHeaders),
    undefined,
    stableMemoOptions,
  );
  const queryHeaders = createMemo(
    () => ('zero' in props ? undefined : props.queryHeaders),
    undefined,
    stableMemoOptions,
  );

  const zero = createMemo(() => {
    if ('zero' in props) {
      return props.zero;
    }

    const [, options] = splitProps(props, [
      'children',
      'auth',
      'context',
      'mutateHeaders',
      'queryHeaders',
    ]);

    const authValue = untrack(() => props.auth);
    const createdZero = new Zero({
      ...options,
      context: context(),
      mutateHeaders: mutateHeaders(),
      queryHeaders: queryHeaders(),
      ...(authValue !== undefined ? {auth: authValue} : {}),
      batchViewUpdates: batch,
    });
    options.init?.(createdZero);
    onCleanup(() => createdZero.close());
    return createdZero;
  });

  const hasAuthProp = createMemo(() => 'auth' in props);
  const auth = createMemo<ZeroOptions<S, MD, Context>['auth']>(() =>
    'auth' in props ? props.auth : undefined,
  );

  let prevHasAuth = hasAuthProp();
  let prevAuth = auth();

  createEffect(() => {
    const currentZero = zero();
    const currentHasAuth = hasAuthProp();
    const currentAuth = auth();

    if (!currentZero || 'zero' in props) {
      prevHasAuth = currentHasAuth;
      prevAuth = currentAuth;
      return;
    }

    if (currentHasAuth !== prevHasAuth || currentAuth !== prevAuth) {
      prevHasAuth = currentHasAuth;
      prevAuth = currentAuth;
      void currentZero.connection.connect({
        auth: currentHasAuth ? currentAuth : undefined,
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
