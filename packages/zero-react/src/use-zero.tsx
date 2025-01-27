import {createContext, useContext} from 'react';
import type {Zero} from '../../zero-client/src/mod.ts';
import type {Schema} from '../../zero-schema/src/mod.ts';

// eslint-disable-next-line @typescript-eslint/naming-convention
const ZeroContext = createContext<unknown | undefined>(undefined);

export function useZero<S extends Schema>(): Zero<S> {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero as Zero<S>;
}

export function createUseZero<S extends Schema>() {
  return () => useZero<S>();
}

export function ZeroProvider<S extends Schema>({
  children,
  zero,
}: {
  children: React.ReactNode;
  zero: Zero<S>;
}) {
  return <ZeroContext.Provider value={zero}>{children}</ZeroContext.Provider>;
}
