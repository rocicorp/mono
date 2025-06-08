import {batch, createContext, createMemo, useContext, type JSX} from 'solid-js';
import {
  Zero,
  type CustomMutatorDefs,
  type Schema,
  type ZeroOptions,
} from '../../zero/src/zero.ts';

const ZeroContext = createContext<(() => Zero<any, any>) | undefined>(
  undefined,
);

export function useZero<
  S extends Schema,
  MD extends CustomMutatorDefs<S> | undefined = undefined,
>(): () => Zero<S, MD> {
  const zero = useContext(ZeroContext);
  if (zero === undefined) {
    throw new Error('useZero must be used within a ZeroProvider');
  }
  return zero;
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
>(props: {children: JSX.Element; options: ZeroOptions<S, MD>}) {
  const zero = createMemo(() => {
    return new Zero({
      ...props.options,
      batchViewUpdates: batch,
    });
  });

  return (
    <ZeroContext.Provider value={zero}>{props.children}</ZeroContext.Provider>
  );
}
