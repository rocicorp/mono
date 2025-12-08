import {deepMerge, type DeepMerge} from '../../../shared/src/deep-merge.ts';
import type {Expand} from '../../../shared/src/expand.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {
  getValueAtPath,
  iterateLeaves,
} from '../../../shared/src/object-traversal.ts';
import type {DefaultSchema} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {validateInput} from '../query/validate-input.ts';
import type {Transaction} from './custom.ts';
import {
  isMutator,
  isMutatorDefinition,
  type AnyMutator,
  type AnyMutatorDefinition,
  type MutationRequest,
  type MutationRequestTypes,
  type Mutator,
  type MutatorDefinition,
  type MutatorTypes,
} from './mutator.ts';

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isMutatorLeaf = (value: unknown): boolean =>
  !isPlainObject(value) || isMutator(value);

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
  // let MD infer freely so defaults aren't erased by a MutatorDefinitions constraint
  const MD,
  S extends Schema = DefaultSchema,
>(
  // we assert types here for clear error messages
  definitions: MD & AssertMutatorDefinitions<MD>,
): MutatorRegistry<EnsureMutatorDefinitions<MD>, S>;

export function defineMutators<
  // same as MD above, but for TBase and TOverrides
  const TBase,
  const TOverrides,
  S extends Schema = DefaultSchema,
>(
  base:
    | MutatorRegistry<EnsureMutatorDefinitions<TBase>, S>
    | (TBase & AssertMutatorDefinitions<TBase>),
  overrides: TOverrides & AssertMutatorDefinitions<TOverrides>,
): MutatorRegistry<
  DeepMerge<
    EnsureMutatorDefinitions<TBase>,
    EnsureMutatorDefinitions<TOverrides>
  >,
  S
>;

export function defineMutators(
  definitionsOrBase: MutatorDefinitions | AnyMutatorRegistry,
  maybeOverrides?: MutatorDefinitions,
): AnyMutatorRegistry {
  function processDefinitions(
    definitions: MutatorDefinitions,
    path: string[],
  ): Record<string | symbol, unknown> {
    const result: Record<string | symbol, unknown> = {
      ['~']: 'MutatorRegistry',
    };

    for (const [key, value] of Object.entries(definitions)) {
      path.push(key);
      const name = path.join('.');

      if (isMutatorDefinition(value)) {
        result[key] = createMutator(name, value);
      } else {
        // Nested definitions
        result[key] = processDefinitions(value as MutatorDefinitions, path);
      }
      path.pop();
    }

    return result;
  }

  if (maybeOverrides !== undefined) {
    // Merge base and overrides
    let base: Record<string | symbol, unknown>;
    if (!isMutatorRegistry(definitionsOrBase)) {
      base = processDefinitions(definitionsOrBase, []);
    } else {
      base = definitionsOrBase;
    }

    const processed = processDefinitions(maybeOverrides, []);

    const merged = deepMerge(base, processed, isMutatorLeaf);
    merged['~'] = 'MutatorRegistry';
    return merged as AnyMutatorRegistry;
  }

  return processDefinitions(
    definitionsOrBase as MutatorDefinitions,
    [],
  ) as AnyMutatorRegistry;
}

/**
 * Gets a Mutator by its dot-separated name from a MutatorRegistry.
 * Returns undefined if not found.
 */
export function getMutator<
  MD extends MutatorDefinitions,
  TSchema extends Schema = DefaultSchema,
>(
  registry: MutatorRegistry<MD, TSchema>,
  name: string,
): FromMutatorTree<MD, TSchema> | undefined {
  const m = getValueAtPath(registry, name, '.');
  return m as FromMutatorTree<MD, TSchema> | undefined;
}

/**
 * Gets a Mutator by its dot-separated name from a MutatorRegistry.
 * Throws if not found.
 */
export function mustGetMutator<
  MD extends MutatorDefinitions,
  TSchema extends Schema = DefaultSchema,
>(
  registry: MutatorRegistry<MD, TSchema>,
  name: string,
): FromMutatorTree<MD, TSchema> {
  const mutator = getMutator(registry, name);
  if (mutator === undefined) {
    throw new Error(`Mutator not found: ${name}`);
  }
  return mutator;
}

/**
 * Checks if a value is a MutatorRegistry.
 */
export function isMutatorRegistry(value: unknown): value is AnyMutatorRegistry {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as Record<string, unknown>)['~'] === 'MutatorRegistry'
  );
}

/**
 * Creates a function that can be used to define mutators with a specific schema.
 */
export function defineMutatorsWithType<
  TSchema extends Schema,
>(): TypedDefineMutators<TSchema> {
  return defineMutators;
}

/**
 * The return type of defineMutatorsWithType. A function matching the
 * defineMutators overloads but with Schema pre-bound.
 */
type TypedDefineMutators<S extends Schema> = {
  // Single definitions
  <MD>(
    definitions: MD & AssertMutatorDefinitions<MD>,
  ): MutatorRegistry<EnsureMutatorDefinitions<MD>, S>;

  // Base and overrides
  <TBase, TOverrides>(
    base:
      | MutatorRegistry<EnsureMutatorDefinitions<TBase>, S>
      | (TBase & AssertMutatorDefinitions<TBase>),
    overrides: TOverrides,
  ): MutatorRegistry<
    DeepMerge<
      EnsureMutatorDefinitions<TBase>,
      EnsureMutatorDefinitions<TOverrides>
    >,
    S
  >;
};

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

/**
 * A tree of MutatorDefinitions, possibly nested.
 */
export type MutatorDefinitions = {
  readonly [key: string]: AnyMutatorDefinition | MutatorDefinitions;
};

export type AssertMutatorDefinitions<MD> = MD extends MutatorDefinitions
  ? unknown
  : never;

export type EnsureMutatorDefinitions<MD> = MD extends MutatorDefinitions
  ? MD
  : never;

/**
 * The result of defineMutators(). A tree of Mutators with a tag for detection.
 */
export type MutatorRegistry<
  MD extends MutatorDefinitions,
  TSchema extends Schema,
> = ToMutatorTree<MD, TSchema> & {
  ['~']: Expand<MutatorRegistryTypes<TSchema>>;
};

export type MutatorRegistryTypes<TSchema extends Schema> = 'MutatorRegistry' & {
  readonly $schema: TSchema;
};

/**
 * A branded type for use in type constraints. Use this instead of
 * `MutatorRegistry<S, C, any>` to avoid TypeScript drilling into
 * the complex ToMutatorTree structure and hitting variance issues.
 */
export type AnyMutatorRegistry = {
  ['~']: Expand<MutatorRegistryTypes<Schema>>;
  [key: string]: unknown;
};

// ----------------------------------------------------------------------------
// Internal
// ----------------------------------------------------------------------------

/**
 * Transforms a MutatorDefinitions into a tree of Mutators.
 * Each MutatorDefinition becomes a Mutator at the same path.
 * Uses TInput for the callable args (TOutput is only used internally for validation).
 */
export type ToMutatorTree<
  MD extends MutatorDefinitions,
  TSchema extends Schema,
> = {
  readonly [K in keyof MD]: MD[K] extends AnyMutatorDefinition
    ? // pull types from the phantom property
      Mutator<
        MD[K]['~']['$input'],
        TSchema,
        MD[K]['~']['$context'],
        MD[K]['~']['$wrappedTransaction']
      >
    : MD[K] extends MutatorDefinitions
      ? ToMutatorTree<MD[K], TSchema>
      : never;
};

export type FromMutatorTree<
  MD extends MutatorDefinitions,
  TSchema extends Schema,
> = {
  readonly [K in keyof MD]: MD[K] extends AnyMutatorDefinition
    ? // pull types from the phantom property
      Mutator<
        ReadonlyJSONValue | undefined, // intentionally left as generic to avoid variance issues
        TSchema,
        MD[K]['~']['$context'],
        MD[K]['~']['$wrappedTransaction']
      >
    : MD[K] extends MutatorDefinitions
      ? FromMutatorTree<MD[K], TSchema>
      : never;
}[keyof MD];

function createMutator<
  ArgsInput extends ReadonlyJSONValue | undefined,
  ArgsOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  C,
  TWrappedTransaction,
>(
  name: string,
  definition: MutatorDefinition<ArgsInput, ArgsOutput, C, TWrappedTransaction>,
): Mutator<ArgsInput, TSchema, C, TWrappedTransaction> {
  const {validator} = definition;

  // fn takes ReadonlyJSONValue args because it's called during rebase (from
  // stored JSON) and on the server (from wire format). Validation happens here.
  const fn = async (options: {
    args: ArgsInput;
    ctx: C;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }): Promise<void> => {
    const validatedArgs = validator
      ? validateInput(name, options.args, validator, 'mutator')
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
  ): MutationRequest<ArgsInput, TSchema, C, TWrappedTransaction> => ({
    'mutator': mutator as unknown as Mutator<
      ArgsInput,
      TSchema,
      C,
      TWrappedTransaction
    >,
    args,
    '~': 'MutationRequest' as MutationRequestTypes<
      ArgsInput,
      TSchema,
      C,
      TWrappedTransaction
    >,
  });
  mutator.mutatorName = name;
  mutator.fn = fn;
  mutator['~'] = 'Mutator' as unknown as MutatorTypes<
    ArgsInput,
    TSchema,
    C,
    TWrappedTransaction
  >;

  return mutator as unknown as Mutator<
    ArgsInput,
    TSchema,
    C,
    TWrappedTransaction
  >;
}

export function* iterateMutators(
  registry: AnyMutatorRegistry,
): Iterable<AnyMutator> {
  yield* iterateLeaves(registry, isMutator);
}
