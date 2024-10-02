import {createContext, useContext} from 'react';
import {type Schema, Zero} from 'zero-client';

// eslint-disable-next-line @typescript-eslint/naming-convention
const ZeroContext = createContext<Zero<Schema> | undefined>(undefined);

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
  return (
    <ZeroContext.Provider value={zero as Zero<Schema>}>
      {children}
    </ZeroContext.Provider>
  );
}
