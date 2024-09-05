import {createContext, useContext} from 'react';
import {QueryDefs, Zero} from 'zero-client';

const ZeroContext = createContext<Zero<QueryDefs> | undefined>(undefined);

export function useZero<Q extends QueryDefs>(): Zero<Q> {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero as Zero<Q>;
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
