import {createContext, useContext} from 'react';
import type {MutatorDefs, QueryDefs, Zero} from 'zero-client';

// eslint-disable-next-line @typescript-eslint/naming-convention
const ZeroContext = createContext<Zero<MutatorDefs, QueryDefs> | undefined>(
  undefined,
);

export function useZero<M extends MutatorDefs, Q extends QueryDefs>(): Zero<
  M,
  Q
> {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero as Zero<M, Q>;
}

export function ZeroProvider<M extends MutatorDefs, Q extends QueryDefs>({
  children,
  zero,
}: {
  children: React.ReactNode;
  zero: Zero<M, Q>;
}) {
  return (
    <ZeroContext.Provider value={zero as Zero<MutatorDefs, QueryDefs>}>
      {children}
    </ZeroContext.Provider>
  );
}
