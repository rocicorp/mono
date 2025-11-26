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
import type {CustomMutatorDefs} from '../../zero-client/src/client/custom.ts';
import type {ZeroOptions} from '../../zero-client/src/client/options.ts';
import {Zero} from '../../zero-client/src/client/zero.ts';
import type {MutatorDefinitions} from '../../zero-types/src/mutator-registry.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {QueryDefinitions} from '../../zql/src/query/query-definitions.ts';

const ZeroContext = createContext<
  // oxlint-disable-next-line no-explicit-any
  Accessor<Zero<any, any, any, any>> | undefined
>(undefined);

const NO_AUTH_SET = Symbol();

export function createZero<
  S extends Schema,
  MD extends
    | MutatorDefinitions<S, Context>
    | CustomMutatorDefs
    | undefined = undefined,
  Context = unknown,
  QD extends QueryDefinitions<S, Context> | undefined = undefined,
>(options: ZeroOptions<S, MD, Context, QD>): Zero<S, MD, Context, QD> {
  const opts = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero(opts);
}

export function useZero<
  S extends Schema,
  MD extends
    | MutatorDefinitions<S, Context>
    | CustomMutatorDefs
    | undefined = undefined,
  Context = unknown,
  QD extends QueryDefinitions<S, Context> | undefined = undefined,
>(): () => Zero<S, MD, Context, QD> {
  const zero = useContext(ZeroContext);

  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero;
}

export function createUseZero<
  S extends Schema,
  MD extends
    | MutatorDefinitions<S, Context>
    | CustomMutatorDefs
    | undefined = undefined,
  Context = unknown,
  QD extends QueryDefinitions<S, Context> | undefined = undefined,
>() {
  return () => useZero<S, MD, Context, QD>();
}

export function ZeroProvider<
  S extends Schema,
  MD extends MutatorDefinitions<S, Context> | CustomMutatorDefs | undefined,
  Context,
  QD extends QueryDefinitions<S, Context> | undefined,
>(
  props: {
    children: JSX.Element;
    init?: (zero: Zero<S, MD, Context, QD>) => void;
  } & (
    | {
        zero: Zero<S, MD, Context, QD>;
      }
    | ZeroOptions<S, MD, Context, QD>
  ),
) {
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
    typeof NO_AUTH_SET | ZeroOptions<S, MD, Context, QD>['auth']
  >(() => ('auth' in props ? props.auth : NO_AUTH_SET));

  let prevAuth: typeof NO_AUTH_SET | ZeroOptions<S, MD, Context, QD>['auth'] =
    NO_AUTH_SET;

  createEffect(() => {
    const currentZero = zero();
    if (!currentZero) {
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
