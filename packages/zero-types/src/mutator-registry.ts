import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import {validateInput} from '../../zql/src/query/validate-input.ts';
import {
  isMutatorDefinition,
  type MutationRequest,
  type Mutator,
  type MutatorDefinition,
} from './mutator.ts';
import type {Schema} from './schema.ts';

/**
 * Creates a MutatorRegistry from a tree of MutatorDefinitions.
 *
 * @example
 * ```typescript
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
  S extends Schema,
  C,
  T extends MutatorDefinitionsTree<S, C>,
>(definitions: T): MutatorRegistry<S, C, T> {
  const tree = buildTree(definitions, []);

  Object.defineProperty(tree, mutatorRegistryTag, {
    value: true,
    writable: false,
    enumerable: false,
    configurable: false,
  });

  return tree as MutatorRegistry<S, C, T>;
}

/**
 * Gets a Mutator by its dot-separated name from a MutatorRegistry.
 * Returns undefined if not found.
 */
// oxlint-disable-next-line no-explicit-any
export function getMutator(
  registry: unknown,
  name: string,
  // oxlint-disable-next-line no-explicit-any
): Mutator<any, any, any, any> | undefined {
  const parts = name.split('.');
  let current: unknown = registry;
  for (const part of parts) {
    if (typeof current !== 'object' || current === null) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[part];
  }
  // oxlint-disable-next-line no-explicit-any
  return current as Mutator<any, any, any, any> | undefined;
}

/**
 * Gets a Mutator by its dot-separated name from a MutatorRegistry.
 * Throws if not found.
 */
// oxlint-disable-next-line no-explicit-any
export function mustGetMutator(
  registry: unknown,
  name: string,
  // oxlint-disable-next-line no-explicit-any
): Mutator<any, any, any, any> {
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
): value is MutatorRegistry<S, C, MutatorDefinitionsTree<S, C>> {
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
// oxlint-disable-next-line no-explicit-any
export type MutatorDefinitionsTree<S extends Schema, C> = {
  readonly [key: string]: // oxlint-disable-next-line no-explicit-any
  MutatorDefinition<S, C, any, any, any> | MutatorDefinitionsTree<S, C>;
};

/**
 * Alias for MutatorDefinitionsTree for backward compatibility.
 */
export type MutatorDefinitions<S extends Schema, C> = MutatorDefinitionsTree<
  S,
  C
>;

/**
 * The result of defineMutators(). A tree of Mutators with a tag for detection.
 */
export type MutatorRegistry<
  S extends Schema,
  C,
  T extends MutatorDefinitionsTree<S, C>,
> = ToMutatorTree<S, C, T> & {
  [mutatorRegistryTag]: true;
};

// ----------------------------------------------------------------------------
// Internal
// ----------------------------------------------------------------------------

const mutatorRegistryTag = Symbol();

/**
 * Transforms a MutatorDefinitionsTree into a tree of Mutators.
 * Each MutatorDefinition becomes a Mutator at the same path.
 * Uses TOutput (the validated type) as TArgs for the resulting Mutator.
 */
type ToMutatorTree<
  S extends Schema,
  C,
  T extends MutatorDefinitionsTree<S, C>,
> = {
  readonly [K in keyof T]: T[K] extends MutatorDefinition<
    S,
    C,
    // oxlint-disable-next-line no-explicit-any
    any, // TInput - we don't need it for the public Mutator type
    infer TOutput,
    infer TWrappedTransaction
  >
    ? Mutator<S, C, TOutput, TWrappedTransaction>
    : T[K] extends MutatorDefinitionsTree<S, C>
      ? ToMutatorTree<S, C, T[K]>
      : never;
};

function buildTree<S extends Schema, C>(
  defs: MutatorDefinitionsTree<S, C>,
  path: string[],
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(defs)) {
    const currentPath = [...path, key];

    if (isMutatorDefinition(value)) {
      const name = currentPath.join('.');
      // oxlint-disable-next-line no-explicit-any
      const mutator = createMutator(
        name,
        // oxlint-disable-next-line no-explicit-any
        value as MutatorDefinition<S, C, any, any, any>,
      );
      result[key] = mutator;
    } else {
      // Nested namespace
      result[key] = buildTree(
        value as MutatorDefinitionsTree<S, C>,
        currentPath,
      );
    }
  }

  return result;
}

// oxlint-disable-next-line no-explicit-any
function createMutator<S extends Schema, C>(
  name: string,
  // oxlint-disable-next-line no-explicit-any
  definition: MutatorDefinition<S, C, any, any, any>,
  // oxlint-disable-next-line no-explicit-any
): Mutator<S, C, any, any> {
  const {validator} = definition;

  // fn takes ReadonlyJSONValue args because it's called during rebase (from
  // stored JSON) and on the server (from wire format). Validation happens here.
  const fn = async (options: {
    args: ReadonlyJSONValue | undefined;
    ctx: C;
    tx: Transaction<S, unknown>;
  }): Promise<void> => {
    const validatedArgs = validator
      ? validateInput(name, options.args, validator, 'mutator')
      : options.args;
    await definition({
      args: validatedArgs,
      ctx: options.ctx,
      tx: options.tx,
    });
  };

  // Create the callable mutator
  // oxlint-disable-next-line no-explicit-any
  const mutator = ((args: unknown): MutationRequest<S, C, any, any> => ({
    mutator,
    args,
    // oxlint-disable-next-line no-explicit-any
  })) as Mutator<S, C, any, any>;
  Object.defineProperty(mutator, 'mutatorName', {
    value: name,
    writable: false,
    enumerable: true,
    configurable: false,
  });
  Object.defineProperty(mutator, 'fn', {
    value: fn,
    writable: false,
    enumerable: true,
    configurable: false,
  });

  return mutator;
}
