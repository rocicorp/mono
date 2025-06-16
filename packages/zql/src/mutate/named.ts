import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {Transaction} from './custom.ts';

export const mutatorsSymbol = Symbol('mutators');

export type NamedMutatorImpl<
  S extends Schema,
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
> = (tx: Transaction<S>, ...args: TArg) => Promise<void>;

export type MutatorMap = Record<
  string,
  (...args: ReadonlyArray<ReadonlyJSONValue>) => Promise<void>
>;

export type MutatorProvider = {
  [mutatorsSymbol]: MutatorMap;
};

export type NamedMutator<
  S extends Schema,
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
> = {
  (tx: Transaction<S> | MutatorProvider, ...args: TArg): Promise<void>;
  mutatorName: string;
};

export function mutator<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
>(name: string, fn: NamedMutatorImpl<S, TArg>): NamedMutator<S, TArg> {
  const ret = (tx: Transaction<S> | MutatorProvider, ...args: TArg) => {
    if (typeof tx === 'object' && mutatorsSymbol in tx) {
      // If we have a mutator provider, we get the mutator from it.
      const mutator = must(tx[mutatorsSymbol][name]);
      return mutator(...args);
    }

    // Otherwise, we assume it's a transaction and call the function directly.
    return fn(tx as Transaction<S>, ...args);
  };

  ret.mutatorName = name;
  return ret as NamedMutator<S, TArg>;
}
