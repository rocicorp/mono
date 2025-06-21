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

/**
 * @deprecated Use {@linkcode ZeroProvider} instead.
 */
export function createZero<S extends Schema, MD extends CustomMutatorDefs<S>>(
  options: ZeroOptions<S, MD>,
): Zero<S, MD> {
  const opts = {
    ...options,
    batchViewUpdates: batch,
  };
  return new Zero(opts);
}

export function useZero<
  S extends Schema,
  MD extends CustomMutatorDefs<S> | undefined = undefined,
>(): () => Zero<S, MD> | undefined {
  const zero = useContext(ZeroContext);

  // TODO: Remove this once we have a way to ensure that useZero is used within a ZeroProvider.
  // if (zero === undefined) {
  //   throw new Error('useZero must be used within a ZeroProvider');
  // }
  return zero ?? (() => undefined);
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
