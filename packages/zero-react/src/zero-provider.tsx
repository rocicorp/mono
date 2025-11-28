import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import {stringCompare} from '../../shared/src/string-compare.ts';
import type {CustomMutatorDefs} from '../../zero-client/src/client/custom.ts';
import type {ZeroOptions} from '../../zero-client/src/client/options.ts';
import {Zero} from '../../zero-client/src/client/zero.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {AnyMutatorRegistry} from '../../zql/src/mutate/mutator-registry.ts';

export const ZeroContext = createContext<unknown | undefined>(undefined);

export function useZero<
  S extends Schema,
  MD extends AnyMutatorRegistry | CustomMutatorDefs | undefined = undefined,
  Context = unknown,
>(): Zero<S, MD, Context> {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero as Zero<S, MD, Context>;
}

export function createUseZero<
  S extends Schema,
  MD extends AnyMutatorRegistry | CustomMutatorDefs | undefined = undefined,
  Context = unknown,
>() {
  return () => useZero<S, MD, Context>();
}

export type ZeroProviderProps<
  S extends Schema,
  MD extends AnyMutatorRegistry | CustomMutatorDefs | undefined,
  Context,
> = (ZeroOptions<S, MD, Context> | {zero: Zero<S, MD, Context>}) & {
  init?: (zero: Zero<S, MD, Context>) => void;
  children: ReactNode;
};

const NO_AUTH_SET = Symbol();

export function ZeroProvider<
  S extends Schema,
  MD extends AnyMutatorRegistry | CustomMutatorDefs | undefined,
  Context,
>({children, init, ...props}: ZeroProviderProps<S, MD, Context>) {
  const isExternalZero = 'zero' in props;

  const [zero, setZero] = useState<Zero<S, MD, Context> | undefined>(
    isExternalZero ? props.zero : undefined,
  );

  const auth = 'auth' in props ? props.auth : NO_AUTH_SET;
  const prevAuthRef = useRef<typeof auth>(auth);

  const keysWithoutAuth = useMemo(
    () =>
      Object.entries(props)
        .filter(([key]) => key !== 'auth')
        .sort(([a], [b]) => stringCompare(a, b))
        .map(([_, value]) => value),
    [props],
  );

  // If Zero is not passed in, we construct it, but only client-side.
  // Zero doesn't really work SSR today so this is usually the right thing.
  // When we support Zero SSR this will either become a breaking change or
  // more likely server support will be opt-in with a new prop on this
  // component.
  useEffect(() => {
    if (isExternalZero) {
      setZero(props.zero);
      return;
    }

    const z = new Zero(props);
    init?.(z);
    setZero(z);

    return () => {
      void z.close();
      setZero(undefined);
    };
    // we intentionally don't include auth in the dependency array
    // to avoid closing zero when auth changes
  }, [init, ...keysWithoutAuth]);

  useEffect(() => {
    if (!zero) return;

    const authChanged = auth !== prevAuthRef.current;

    if (authChanged) {
      prevAuthRef.current = auth;
      void zero.connection.connect({
        auth: auth === NO_AUTH_SET ? undefined : auth,
      });
    }
  }, [auth, zero]);

  return (
    zero && <ZeroContext.Provider value={zero}>{children}</ZeroContext.Provider>
  );
}
