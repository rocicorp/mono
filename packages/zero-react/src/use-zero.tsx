import {createContext, useContext} from 'react';
import type {Zero} from '../../zero-client/src/client/zero.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {CustomMutatorDefs} from '../../zero-client/src/client/custom.ts';

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

export function ZeroProvider<
  S extends Schema,
  MD extends CustomMutatorDefs<S> | undefined = undefined,
>({children, zero}: {children: React.ReactNode; zero: Zero<S, MD>}) {
  return <ZeroContext.Provider value={zero}>{children}</ZeroContext.Provider>;
}
