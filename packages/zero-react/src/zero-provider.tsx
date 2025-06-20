import {
  createContext,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from 'react';
import {Zero} from '../../zero-client/src/client/zero.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {CustomMutatorDefs} from '../../zero-client/src/client/custom.ts';
import type {ZeroOptions} from '../../zero-client/src/client/options.ts';

// eslint-disable-next-line @typescript-eslint/naming-convention
const ZeroContext = createContext<unknown | undefined>(undefined);

export function useZero<
  S extends Schema,
  MD extends CustomMutatorDefs<S> | undefined = undefined,
>(): Zero<S, MD> {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero as Zero<S, MD>;
}

export function createUseZero<
  S extends Schema,
  MD extends CustomMutatorDefs<S> | undefined = undefined,
>() {
  return () => useZero<S, MD>();
}

export type ZeroProviderProps<
  S extends Schema,
  MD extends CustomMutatorDefs<S> | undefined = undefined,
> = (ZeroOptions<S, MD> | {zero: Zero<S, MD>}) & {
  init?: (zero: Zero<S, MD>) => void;
  children: ReactNode;
};

export function ZeroProvider<
  S extends Schema,
  MD extends CustomMutatorDefs<S> | undefined = undefined,
>({children, init, ...props}: ZeroProviderProps<S, MD>) {
  const [zero, setZero] = useState<Zero<S, MD> | undefined>(
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
      void zero?.close();
      setZero(undefined);
    };
  }, [init, ...Object.values(props)]);

  return (
    zero && <ZeroContext.Provider value={zero}>{children}</ZeroContext.Provider>
  );
}
