import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import {isJSONEqual} from '../../shared/src/json.ts';
import {stringCompare} from '../../shared/src/string-compare.ts';
import {
  Zero,
  type CustomMutatorDefs,
  type DefaultContext,
  type DefaultSchema,
  type Schema,
  type ZeroOptions,
} from './zero.ts';

// oxlint-disable-next-line no-explicit-any
export const ZeroContext = createContext<Zero<any, any, any> | undefined>(
  undefined,
);

export function useZero<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
>(): Zero<S, MD, Context> {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero as Zero<S, MD, Context>;
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

export type ZeroProviderProps<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
> = (ZeroOptions<S, MD, Context> | {zero: Zero<S, MD, Context>}) & {
  init?: (zero: Zero<S, MD, Context>) => void;
  children: ReactNode;
};

function useStableJSONProp<T>(value: T): T {
  const stableRef = useRef(value);
  if (!isJSONEqual(stableRef.current, value)) {
    stableRef.current = value;
  }
  return stableRef.current;
}

export function ZeroProvider<
  S extends Schema = DefaultSchema,
  MD extends CustomMutatorDefs | undefined = undefined,
  Context = DefaultContext,
>({children, init, ...initialProps}: ZeroProviderProps<S, MD, Context>) {
  const isZeroInProps = 'zero' in initialProps;
  const hasAuthProp = 'auth' in initialProps;

  const {auth, context, mutateHeaders, queryHeaders, ...otherProps} = useMemo(
    () =>
      isZeroInProps
        ? ({} as Partial<ZeroOptions<S, MD, Context>>)
        : initialProps,
    [isZeroInProps, initialProps],
  );

  const stableAuth = useStableJSONProp({hasAuthProp, auth});
  const stableContext = useStableJSONProp(context);
  const stableMutateHeaders = useStableJSONProp(mutateHeaders);
  const stableQueryHeaders = useStableJSONProp(queryHeaders);
  const externalZero = isZeroInProps ? initialProps.zero : undefined;

  const [zero, setZero] = useState<Zero<S, MD, Context> | undefined>(
    externalZero,
  );

  const prevAuthRef = useRef(stableAuth);

  const zeroDepsWithoutAuth = useMemo(() => {
    const depsByKey: Record<string, unknown> = {
      ...otherProps,
      context: stableContext,
      mutateHeaders: stableMutateHeaders,
      queryHeaders: stableQueryHeaders,
    };

    const deps: unknown[] = [];
    for (const key of Object.keys(depsByKey).sort(stringCompare)) {
      deps.push(depsByKey[key]);
    }
    return deps;
  }, [otherProps, stableContext, stableMutateHeaders, stableQueryHeaders]);

  // If Zero is not passed in, we construct it, but only client-side.
  // Zero doesn't really work SSR today so this is usually the right thing.
  // When we support Zero SSR this will either become a breaking change or
  // more likely server support will be opt-in with a new prop on this
  // component.
  // TODO(0xcadams): this client-side only conditional render results
  // in all children being conditional on zero provider,
  // which is not what most people want.
  useEffect(() => {
    if (isZeroInProps) {
      setZero(initialProps.zero);
      return;
    }

    const z = new Zero(initialProps);
    init?.(z);
    setZero(z);

    return () => {
      void z.close();
      setZero(undefined);
    };
    // use stable props here to avoid unnecessary zero reconnects
  }, [init, isZeroInProps, externalZero, ...zeroDepsWithoutAuth]);

  useEffect(() => {
    if (!zero || isZeroInProps) return;

    const authChanged = stableAuth !== prevAuthRef.current;

    if (authChanged) {
      prevAuthRef.current = stableAuth;
      void zero.connection.connect({
        auth: stableAuth.hasAuthProp ? stableAuth.auth : undefined,
      });
    }
  }, [stableAuth, zero]);

  return (
    zero && <ZeroContext.Provider value={zero}>{children}</ZeroContext.Provider>
  );
}
