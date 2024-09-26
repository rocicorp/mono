import {createContext, useContext} from 'react';
import {type QueryDefs, Zero} from 'zero-client';

// eslint-disable-next-line @typescript-eslint/naming-convention
const ZeroContext = createContext<Zero<QueryDefs> | undefined>(undefined);

export function useZero<Q extends QueryDefs>(): Zero<Q> {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero as Zero<Q>;
}

export function createUseZero<Q extends QueryDefs>() {
  return () => useZero<Q>();
}

export function ZeroProvider<Q extends QueryDefs>({
  children,
  zero,
}: {
  children: React.ReactNode;
  zero: Zero<Q>;
}) {
  return (
    <ZeroContext.Provider value={zero as Zero<QueryDefs>}>
      {children}
    </ZeroContext.Provider>
  );
}
