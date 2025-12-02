import {deepMerge, type DeepMerge} from '../../../shared/src/deep-merge.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {
  getValueAtPath,
  iterateLeaves,
} from '../../../shared/src/object-traversal.ts';
import type {
  DefaultContext,
  DefaultSchema,
} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {validateInput} from '../query/validate-input.ts';
import type {Transaction} from './custom.ts';
import {
  isMutator,
  isMutatorDefinition,
  type AnyMutator,
  type MutationRequest,
  type Mutator,
  type MutatorDefinition,
} from './mutator.ts';

/**
 * Creates a MutatorRegistry from a tree of MutatorDefinitions,
 * optionally extending a base MutatorRegistry.
 *
 * @example
 * ```ts
 * // Create a new registry
 * const mutators = defineMutators({
 *   user: {
 *     create: defineMutator(...),
 *     delete: defineMutator(...),
 *   },
 *   post: {
 *     publish: defineMutator(...),
 *   },
 * });
 *
 * // Extend an existing registry (e.g., for server-side overrides)
 * const serverMutators = defineMutators(mutators, {
 *   user: {
 *     create: defineMutator(...),  // overrides mutators.user.create
 *   },
 *   // post.publish is inherited from mutators
 * });
 *
 * // Access mutators by path
 * const mr = mutators.user.create({name: 'Alice'});
 *
 * // Execute on client
 * zero.mutate(mr);
 *
 * // Execute on server
 * mr.mutator.fn({tx, ctx, args: mr.args});
 *
 * // Lookup by name (for server-side dispatch)
 * const mutator = getMutator(mutators, 'user.create');
 * ```
 */
export function defineMutators<
  T extends MutatorDefinitions<S, C>,
  S extends Schema = DefaultSchema,
  C = DefaultContext,
>(definitions: T): MutatorRegistry<S, C, T>;

export function defineMutators<
  TBase extends MutatorDefinitions<S, C>,
  TOverrides extends MutatorDefinitions<S, C>,
  S extends Schema = DefaultSchema,
  C = DefaultContext,
>(
  base: MutatorRegistry<S, C, TBase>,
  overrides: TOverrides,
): MutatorRegistry<S, C, DeepMerge<TBase, TOverrides>>;

export function defineMutators<
  TBase extends MutatorDefinitions<S, C>,
  TOverrides extends MutatorDefinitions<S, C>,
  S extends Schema = DefaultSchema,
  C = DefaultContext,
>(
  base: TBase,
  overrides: TOverrides,
): MutatorRegistry<S, C, DeepMerge<TBase, TOverrides>>;

export function defineMutators<S extends Schema, C>(
  definitionsOrBase: MutatorDefinitions<S, C> | AnyMutatorRegistry,
  maybeOverrides?: MutatorDefinitions<S, C>,
): AnyMutatorRegistry {
  function processDefinitions(
    definitions: MutatorDefinitions<S, C>,
    path: string[],
  ): Record<string | symbol, unknown> {
    const result: Record<string | symbol, unknown> = {
      [mutatorRegistryTag]: true,
    };

    for (const [key, value] of Object.entries(definitions)) {
      path.push(key);
      const name = path.join('.');

      if (isMutatorDefinition(value)) {
        result[key] = createMutator(name, value);
      } else {
        // Nested definitions
        result[key] = processDefinitions(
          value as MutatorDefinitions<S, C>,
          path,
        );
      }
      path.pop();
    }

    return result;
  }

  if (maybeOverrides !== undefined) {
    // Merge base and overrides
    let base: Record<string | symbol, unknown>;
    if (!isMutatorRegistry(definitionsOrBase)) {
      base = processDefinitions(
        definitionsOrBase as MutatorDefinitions<S, C>,
        [],
      );
    } else {
      base = definitionsOrBase;
    }

    const processed = processDefinitions(maybeOverrides, []);

    const merged = deepMerge(base, processed, isMutator) as Record<
      string | symbol,
      unknown
    >;
    // deepMerge doesn't copy symbols, so we need to add the tag
    merged[mutatorRegistryTag] = true;
    return merged as AnyMutatorRegistry;
  }

  return processDefinitions(
    definitionsOrBase as MutatorDefinitions<S, C>,
    [],
  ) as AnyMutatorRegistry;
}

/**
 * Gets a Mutator by its dot-separated name from a MutatorRegistry.
 * Returns undefined if not found.
 */
export function getMutator(
  registry: AnyMutatorRegistry,
  name: string,
): AnyMutator | undefined {
  const m = getValueAtPath(registry, name, '.');
  return m as AnyMutator | undefined;
}

/**
 * Gets a Mutator by its dot-separated name from a MutatorRegistry.
 * Throws if not found.
 */
export function mustGetMutator(
  registry: AnyMutatorRegistry,
  name: string,
): AnyMutator {
  const mutator = getMutator(registry, name);
  if (mutator === undefined) {
    throw new Error(`Mutator not found: ${name}`);
  }
  return mutator;
}

/**
 * Checks if a value is a MutatorRegistry.
 */
export function isMutatorRegistry<S extends Schema, C>(
  value: unknown,
): value is MutatorRegistry<S, C, MutatorDefinitions<S, C>> {
  return (
    typeof value === 'object' && value !== null && mutatorRegistryTag in value
  );
}

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

/**
 * A tree of MutatorDefinitions, possibly nested.
 */
export type MutatorDefinitions<S extends Schema, C> = {
  readonly [key: string]: // oxlint-disable-next-line no-explicit-any
  MutatorDefinition<any, any, S, C, any> | MutatorDefinitions<S, C>;
};

/**
 * The result of defineMutators(). A tree of Mutators with a tag for detection.
 */
export type MutatorRegistry<
  S extends Schema,
  C,
  T extends MutatorDefinitions<S, C>,
> = ToMutatorTree<S, C, T> & {
  [mutatorRegistryTag]: true;
};

/**
 * A branded type for use in type constraints. Use this instead of
 * `MutatorRegistry<S, C, any>` to avoid TypeScript drilling into
 * the complex ToMutatorTree structure and hitting variance issues.
 */
export type AnyMutatorRegistry = {[mutatorRegistryTag]: true} & Record<
  string,
  unknown
>;

// ----------------------------------------------------------------------------
// Internal
// ----------------------------------------------------------------------------

const mutatorRegistryTag = Symbol('mutatorRegistry');

/**
 * Transforms a MutatorDefinitions into a tree of Mutators.
 * Each MutatorDefinition becomes a Mutator at the same path.
 * Uses TInput for the callable args (TOutput is only used internally for validation).
 */
type ToMutatorTree<S extends Schema, C, T extends MutatorDefinitions<S, C>> = {
  readonly [K in keyof T]: T[K] extends MutatorDefinition<
    infer TInput,
    // oxlint-disable-next-line no-explicit-any
    any, // TOutput - only used internally for validation
    S,
    C,
    infer TWrappedTransaction
  >
    ? Mutator<TInput, S, C, TWrappedTransaction>
    : T[K] extends MutatorDefinitions<S, C>
      ? ToMutatorTree<S, C, T[K]>
      : never;
};

function createMutator<
  S extends Schema,
  C,
  ArgsInput extends ReadonlyJSONValue | undefined,
  ArgsOutput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction,
>(
  name: string,
  definition: MutatorDefinition<
    ArgsInput,
    ArgsOutput,
    S,
    C,
    TWrappedTransaction
  >,
): Mutator<ArgsInput, S, C, TWrappedTransaction> {
  const {validator} = definition;

  // fn takes ReadonlyJSONValue args because it's called during rebase (from
  // stored JSON) and on the server (from wire format). Validation happens here.
  const fn = async (options: {
    args: ArgsInput;
    ctx: C;
    tx: Transaction<S, TWrappedTransaction>;
  }): Promise<void> => {
    const validatedArgs = validator
      ? validateInput<ArgsInput, ArgsOutput>(
          name,
          options.args,
          validator,
          'mutator',
        )
      : (options.args as unknown as ArgsOutput);
    await definition({
      args: validatedArgs,
      ctx: options.ctx,
      tx: options.tx,
    });
  };

  // Create the callable mutator
  const mutator = (
    args: ArgsInput,
  ): MutationRequest<ArgsInput, S, C, Transaction<S, TWrappedTransaction>> => ({
    mutator: mutator as unknown as Mutator<
      ArgsInput,
      S,
      C,
      Transaction<S, TWrappedTransaction>
    >,
    args,
  });
  mutator.mutatorName = name;
  mutator.fn = fn;

  return mutator as unknown as Mutator<
    ArgsInput,
    S,
    C,
    Transaction<S, TWrappedTransaction>
  >;
}

export function* iterateMutators(
  registry: AnyMutatorRegistry,
): Iterable<AnyMutator> {
  yield* iterateLeaves(registry, isMutator);
}
