import {createContext, useContext} from 'react';
import type {Zero} from '../../zero-client/src/mod.js';
import type {Schema} from '../../zero-schema/src/mod.js';

// eslint-disable-next-line @typescript-eslint/naming-convention
const ZeroContext = createContext<unknown | undefined>(undefined);

export function useZero<S extends Schema, Z extends Zero<S> = Zero<S>>(): Z {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero as Z;
}

export function createUseZero<S extends Schema, Z extends Zero<S> = Zero<S>>() {
  return () => useZero<S, Z>();
}

export function ZeroProvider<S extends Schema, Z extends Zero<S> = Zero<S>>({
  children,
  zero,
}: {
  children: React.ReactNode;
  zero: Z;
}) {
  return <ZeroContext.Provider value={zero}>{children}</ZeroContext.Provider>;
}
