import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from 'react';
import type {CustomMutatorDefs} from '../../zero-client/src/client/custom.ts';
import type {ZeroOptions} from '../../zero-client/src/client/options.ts';
import {Zero} from '../../zero-client/src/client/zero.ts';
import type {Schema} from '../../zero-types/src/schema.ts';

const ZeroContext = createContext<unknown | undefined>(undefined);

export function useZero<
  S extends Schema,
  MD extends CustomMutatorDefs,
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
  MD extends CustomMutatorDefs,
  Context = unknown,
>() {
  return () => useZero<S, MD, Context>();
}

export type ZeroProviderProps<
  S extends Schema,
  MD extends CustomMutatorDefs,
  Context,
> = (ZeroOptions<S, MD, Context> | {zero: Zero<S, MD, Context>}) & {
  init?: (zero: Zero<S, MD, Context>) => void;
  children: ReactNode;
};

export function ZeroProvider<
  S extends Schema,
  MD extends CustomMutatorDefs,
  TContext,
>({children, init, ...props}: ZeroProviderProps<S, MD, TContext>) {
  const [zero, setZero] = useState<Zero<S, MD, TContext> | undefined>(
    'zero' in props ? props.zero : undefined,
  );

  // If Zero is not passed in, we construct it, but only client-side.
  // Zero doesn't really work SSR today so this is usually the right thing.
  // When we support Zero SSR this will either become a breaking change or
  // more likely server support will be opt-in with a new prop on this
  // component.
  useEffect(() => {
    if ('zero' in props) {
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
  }, [init, ...Object.values(props)]);

  return (
    zero && <ZeroContext.Provider value={zero}>{children}</ZeroContext.Provider>
  );
}
