import type {DeepMerge} from '../../../shared/src/deep-merge.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {
  customMutatorKey,
  type Transaction,
} from '../../../zql/src/mutate/custom.ts';
import type {DBMutator} from './crud.ts';
import type {CustomMutatorDefs, MutatorResult} from './custom.ts';
import type {MutatorProxy} from './mutator-proxy.ts';

/**
 * Creates and populates a mutate property object by processing mutator definitions recursively.
 *
 * This function traverses through mutator definitions (either schema-based or custom) and builds
 * a corresponding object structure where each mutator is wrapped by the mutator proxy. It handles
 * both flat mutator functions and nested mutator definition objects.
 *
 * @template S - The schema type that defines the structure of the data
 * @template C - The context type used by mutators, defaults to unknown
 *
 * @param mutators - The mutator definitions to process, can be schema-based or custom mutator definitions
 * @param mutatorProxy - The proxy object responsible for wrapping mutators with additional functionality
 * @param mutateObject - The target object to populate with wrapped mutators
 * @param replicacheMutate - The source object containing the actual mutator implementations to wrap
 *
 * @returns void - This function mutates the mutateObject parameter directly
 *
 * @remarks
 * The function recursively processes nested mutator structures, creating corresponding nested objects
 * in the mutateObject. For leaf mutators (functions or mutator definitions), it generates a full key
 * using different separators ('.' for mutator definitions, '|' for custom functions) and wraps them
 * using the mutator proxy.
 */
export function addCustomMutatorsProperties(
  mutators: CustomMutatorDefs,
  mutatorProxy: MutatorProxy,
  mutateObject: Record<string, unknown>,
  replicacheMutate: Record<string, unknown>,
): void {
  const processMutators = (
    mutators: CustomMutatorDefs,
    path: string[],
    mutateObject: Record<string, unknown>,
  ) => {
    for (const [key, mutator] of Object.entries(mutators)) {
      path.push(key);
      if (typeof mutator === 'function') {
        const fullKey = customMutatorKey('|', path);
        mutateObject[key] = mutatorProxy.wrapCustomMutator(
          fullKey,
          must(replicacheMutate[fullKey]) as unknown as (
            ...args: unknown[]
          ) => MutatorResult,
        );
      } else {
        // Nested namespace - recursive build and process.
        let existing = mutateObject[key];
        if (existing === undefined) {
          existing = {};
          mutateObject[key] = existing;
        }
        processMutators(
          mutator as CustomMutatorDefs,
          path,
          existing as Record<string, unknown>,
        );
      }
      path.pop();
    }
  };

  processMutators(mutators, [], mutateObject);
}

/**
 * Builds the mutate type from legacy CustomMutatorDefs, handling arbitrary nesting.
 * Each node can be either a CustomMutatorImpl function or a namespace containing more mutators.
 */
type MakeFromMutatorDefinitions<
  S extends Schema,
  MD extends CustomMutatorDefs,
  C,
> = {
  readonly [K in keyof MD]: MD[K] extends (
    tx: Transaction<S>,
    ...args: infer Args
  ) => Promise<void>
    ? (...args: Args) => MutatorResult
    : MD[K] extends CustomMutatorDefs
      ? MakeFromMutatorDefinitions<S, MD[K], C>
      : never;
};

export type MakeMutatePropertyType<
  S extends Schema,
  MD extends CustomMutatorDefs | undefined,
  C,
> = MD extends CustomMutatorDefs
  ? S['enableLegacyMutators'] extends true
    ? DeepMerge<DBMutator<S>, MakeFromMutatorDefinitions<S, MD, C>>
    : MakeFromMutatorDefinitions<S, MD, C>
  : DBMutator<S>;
