import type {LogContext} from '@rocicorp/logger';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {customMutatorKey} from '../../../zql/src/mutate/custom.ts';
import type {CustomMutatorDefs, CustomMutatorImpl} from './custom.ts';
import {isMutatorDefinition, type MutatorDefinition} from './define-mutator.ts';
import type {MutatorDefinitions} from './mutator-definitions.ts';

import type {MutatorDefs} from '../../../replicache/src/types.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {CRUD_MUTATION_NAME} from '../../../zero-protocol/src/push.ts';
import {validateInput} from '../../../zql/src/query/validate-input.ts';
import {ClientErrorKind} from './client-error-kind.ts';
import {type CRUDMutator, makeCRUDMutator} from './crud.ts';
import {
  makeReplicacheMutator as makeReplicacheMutatorLegacy,
  TransactionImpl,
} from './custom.ts';
import {ClientError} from './error.ts';
import type {WriteTransaction} from './replicache-types.ts';

export function extendReplicacheMutators<S extends Schema, C>(
  lc: LogContext,
  contextHolder: {context: C},
  mutators: MutatorDefinitions<S, C> | CustomMutatorDefs,
  schema: S,
  mutateObject: Record<string, unknown>,
): void {
  // Recursively process mutator definitions at arbitrary depth
  const processMutators = (
    mutators: MutatorDefinitions<S, C> | CustomMutatorDefs,
    path: string[],
  ) => {
    for (const [key, mutator] of Object.entries(mutators)) {
      path.push(key);
      if (isMutatorDefinition(mutator)) {
        const fullKey = customMutatorKey('.', path);
        mutateObject[fullKey] = makeReplicacheMutator(
          lc,
          mutator,
          fullKey,
          schema,
          contextHolder,
        );
      } else if (typeof mutator === 'function') {
        // oxlint-disable-next-line no-explicit-any
        mutator satisfies CustomMutatorImpl<any>;
        const fullKey = customMutatorKey('|', path);
        mutateObject[fullKey] = makeReplicacheMutatorLegacy(
          mutator,
          mutator,
          schema,
        );
      } else {
        processMutators(mutator, path);
      }
      path.pop();
    }
  };

  processMutators(mutators, []);
}

function makeReplicacheMutator<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction,
>(
  lc: LogContext,
  mutator: MutatorDefinition<
    TSchema,
    TContext,
    TInput,
    TOutput,
    TWrappedTransaction
  >,
  queryName: string,
  schema: TSchema,
  contextHolder: {context: TContext},
): (repTx: WriteTransaction, args: ReadonlyJSONValue) => Promise<void> {
  const {validator} = mutator;
  const validate = validator
    ? (args: TInput) => validateInput(queryName, args, validator, 'query')
    : (args: TInput) => args;

  return async (
    repTx: WriteTransaction,
    args: ReadonlyJSONValue,
  ): Promise<void> => {
    const tx = new TransactionImpl(lc, repTx, schema);
    await mutator({
      args: validate(args as TInput) as TOutput,
      ctx: contextHolder.context,
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
 * @param contextHolder - An object containing the context to be passed to mutators
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
  mutators: MutatorDefinitions<S, C> | CustomMutatorDefs | undefined,
  contextHolder: {context: C},
  lc: LogContext,
): MutatorDefs & {_zero_crud: CRUDMutator} {
  const {enableLegacyMutators = true} = schema;

  const replicacheMutators = {
    [CRUD_MUTATION_NAME]: enableLegacyMutators
      ? makeCRUDMutator(schema)
      : () =>
          Promise.reject(
            new ClientError({
              kind: ClientErrorKind.Internal,
              message: 'Zero CRUD mutators are not enabled.',
            }),
          ),
  };

  if (mutators) {
    extendReplicacheMutators(
      lc,
      contextHolder,
      mutators,
      schema,
      replicacheMutators,
    );
  }

  return replicacheMutators;
}
