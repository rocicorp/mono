import type {LogContext} from '@rocicorp/logger';
import type {MutatorDefs} from '../../../replicache/src/types.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {CRUD_MUTATION_NAME} from '../../../zero-protocol/src/push.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {customMutatorKey} from '../../../zql/src/mutate/custom.ts';
import {
  isMutatorRegistry,
  type AnyMutatorRegistry,
} from '../../../zql/src/mutate/mutator-registry.ts';
import {type Mutator} from '../../../zql/src/mutate/mutator.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {makeCRUDMutator, type CRUDMutator} from './crud.ts';
import type {CustomMutatorDefs, CustomMutatorImpl} from './custom.ts';
import {
  makeReplicacheMutator as makeReplicacheMutatorLegacy,
  TransactionImpl,
} from './custom.ts';
import {ClientError} from './error.ts';
import type {WriteTransaction} from './replicache-types.ts';

export function extendReplicacheMutators<S extends Schema, C>(
  lc: LogContext,
  context: C,
  mutators: AnyMutatorRegistry | CustomMutatorDefs,
  schema: S,
  mutateObject: Record<string, unknown>,
): void {
  // Recursively process mutator definitions at arbitrary depth
  const processMutators = (mutators: object, path: string[]) => {
    for (const [key, mutator] of Object.entries(mutators)) {
      if (key === '~') {
        // Skip phantom type
        continue;
      }

      const nextPath = [...path, key];
      if (isMutator(mutator)) {
        const fullKey = customMutatorKey('.', nextPath);
        mutateObject[fullKey] = makeReplicacheMutator(
          lc,
          mutator,
          schema,
          context,
        );
      } else if (typeof mutator === 'function') {
        const fullKey = customMutatorKey('|', nextPath);
        mutateObject[fullKey] = makeReplicacheMutatorLegacy(
          lc,
          // oxlint-disable-next-line no-explicit-any
          mutator as CustomMutatorImpl<any>,
          schema,
          context,
        );
      } else if (mutator !== null && typeof mutator === 'object') {
        processMutators(mutator, nextPath);
      }
    }
  };

  processMutators(mutators, []);
}

function makeReplicacheMutator<
  TArgs extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
>(
  lc: LogContext,
  mutator: Mutator<TArgs, TSchema, TContext, TWrappedTransaction>,
  schema: TSchema,
  context: TContext,
): (repTx: WriteTransaction, args: ReadonlyJSONValue) => Promise<void> {
  return async (
    repTx: WriteTransaction,
    args: ReadonlyJSONValue,
  ): Promise<void> => {
    const tx = new TransactionImpl(lc, repTx, schema);
    // fn does input validation internally
    await mutator.fn({
      args: args as TArgs,
      ctx: context,
      tx: tx,
    });
  };
}

/**
 * Creates Replicache mutators from mutator definitions.
 *
 * This function processes mutator definitions at arbitrary depth, supporting both
 * new-style mutator definitions and legacy custom mutator implementations. It creates
 * a mutator object with the CRUD mutator and any provided custom mutators, with keys
 * generated based on their path in the mutator definition hierarchy.
 *
 * @template S - The schema type that defines the structure of the data
 * @template C - The type of the context object passed to mutators
 *
 * @param schema - The schema instance used for validation and type checking
 * @param mutators - The mutator definitions to process, can be nested objects or custom mutator definitions
 * @param context - The context to be passed to mutators
 * @param lc - The log context used for logging operations
 *
 * @returns A mutator definitions object containing the CRUD mutator and any custom mutators
 *
 * @remarks
 * - New-style mutator definitions use '.' as a separator in their keys
 * - Legacy custom mutator implementations use '|' as a separator in their keys
 * - The CRUD mutator can be disabled by setting `enableLegacyMutators: false` in the schema
 */
export function makeReplicacheMutators<const S extends Schema, C>(
  schema: S,
  mutators: AnyMutatorRegistry | CustomMutatorDefs | undefined,
  context: C,
  lc: LogContext,
): MutatorDefs & {_zero_crud: CRUDMutator} {
  const {enableLegacyMutators = false} = schema;

  const replicacheMutators = {
    [CRUD_MUTATION_NAME]: enableLegacyMutators
      ? makeCRUDMutator(schema)
      : // TODO(arv): This code is unreachable since the public API prevents
        // calling CRUD mutators when enableLegacyMutators is false. Remove this.
        () =>
          Promise.reject(
            new ClientError({
              kind: ClientErrorKind.Internal,
              message: 'Zero CRUD mutators are not enabled.',
            }),
          ),
  };

  if (mutators) {
    if (isMutatorRegistry(mutators)) {
      extendFromMutatorRegistry(
        lc,
        context,
        mutators,
        schema,
        replicacheMutators,
      );
    } else {
      extendReplicacheMutators(
        lc,
        context,
        mutators as CustomMutatorDefs,
        schema,
        replicacheMutators,
      );
    }
  }

  return replicacheMutators;
}

/**
 * Checks if a value is a Mutator (from MutatorRegistry).
 * Mutators have `mutatorName` and `fn` properties.
 */
function isMutator(
  value: unknown,
  // oxlint-disable-next-line no-explicit-any
): value is Mutator<any, any, any> {
  return (
    typeof value === 'function' &&
    'mutatorName' in value &&
    typeof value.mutatorName === 'string' &&
    'fn' in value &&
    typeof value.fn === 'function'
  );
}

/**
 * Extends replicache mutators from a MutatorRegistry.
 * Walks the registry tree and wraps each Mutator.fn for Replicache.
 */
function extendFromMutatorRegistry<S extends Schema, C>(
  lc: LogContext,
  context: C,
  registry: AnyMutatorRegistry,
  schema: S,
  mutateObject: Record<string, unknown>,
): void {
  const walk = (node: unknown) => {
    if (typeof node !== 'object' || node === null) {
      return;
    }
    for (const value of Object.values(node)) {
      if (isMutator(value)) {
        // Mutator.fn already handles validation internally
        mutateObject[value.mutatorName] = (
          repTx: WriteTransaction,
          args: ReadonlyJSONValue,
        ): Promise<void> => {
          const tx = new TransactionImpl(lc, repTx, schema);
          return value.fn({args, ctx: context, tx});
        };
      } else if (typeof value === 'object' && value !== null) {
        // Nested namespace
        walk(value);
      }
    }
  };
  walk(registry);
}
