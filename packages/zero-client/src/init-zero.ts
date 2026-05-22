import type {
  BaseDefaultContext,
  BaseDefaultSchema,
  DefaultSchema,
} from '../../zero-types/src/default-types.ts';
import type {
  ClientTransaction,
  ServerTransaction,
  Transaction,
} from '../../zql/src/mutate/custom.ts';
import {
  defineMutatorsWithType,
  type TypedDefineMutators,
} from '../../zql/src/mutate/mutator-registry.ts';
import {
  defineMutatorWithType,
  type TypedDefineMutator,
} from '../../zql/src/mutate/mutator.ts';
import {
  defineQueriesWithType,
  defineQueryWithType,
  type TypedDefineQueries,
  type TypedDefineQuery,
} from '../../zql/src/query/query-registry.ts';
import type {Row} from '../../zql/src/query/query.ts';
import type {ZeroOptions} from './client/options.ts';
import {Zero} from './client/zero.ts';

export type TypedZero<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TSchemaBound extends boolean = false,
> = new (
  options: TSchemaBound extends true
    ? Omit<ZeroOptions<S, undefined, C>, 'schema'>
    : ZeroOptions<S, undefined, C>,
) => Zero<S, undefined, C>;

export type InitZeroOptions<S extends BaseDefaultSchema> = {
  readonly schema: S;
};

type SchemaBinding<
  S extends BaseDefaultSchema,
  TSchemaBound extends boolean,
> = TSchemaBound extends true
  ? {
      readonly schema: S;
    }
  : unknown;

export type InitZeroResult<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TWrappedTransaction,
  TSchemaBound extends boolean = false,
> = {
  readonly '~': InitZeroTypes<S, C, TWrappedTransaction>;
  readonly 'Zero': TypedZero<S, C, TSchemaBound>;
  readonly 'defineMutator': TypedDefineMutator<S, C, TWrappedTransaction>;
  readonly 'defineMutators': TypedDefineMutators<S>;
  readonly 'defineQuery': TypedDefineQuery<S, C>;
  readonly 'defineQueries': TypedDefineQueries<S>;
} & SchemaBinding<S, TSchemaBound>;

export type InitZeroTypes<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TWrappedTransaction,
> = 'InitZero' & {
  readonly $schema: S;
  readonly $context: C;
  readonly $wrappedTransaction: TWrappedTransaction;
  readonly $row: Row<S>;
  readonly $transaction: Transaction<S, TWrappedTransaction>;
  readonly $clientTransaction: ClientTransaction<S>;
  readonly $serverTransaction: ServerTransaction<S, TWrappedTransaction>;
  readonly $zero: Zero<S, undefined, C>;
};

/**
 * Returns typed Zero helpers without relying on module augmentation.
 */
export function initZero<
  S extends BaseDefaultSchema = DefaultSchema,
  C extends BaseDefaultContext = unknown,
  TWrappedTransaction = unknown,
>(): InitZeroResult<S, C, TWrappedTransaction>;
export function initZero<
  S extends BaseDefaultSchema = DefaultSchema,
  C extends BaseDefaultContext = unknown,
  TWrappedTransaction = unknown,
>(options: InitZeroOptions<S>): InitZeroResult<S, C, TWrappedTransaction, true>;

export function initZero<
  S extends BaseDefaultSchema = DefaultSchema,
  C extends BaseDefaultContext = unknown,
  TWrappedTransaction = unknown,
>(
  options?: InitZeroOptions<S>,
): InitZeroResult<S, C, TWrappedTransaction, boolean> {
  const TypedZero = options
    ? (() => {
        const {schema} = options;
        return class extends Zero<S, undefined, C> {
          constructor(
            zeroOptions: Omit<ZeroOptions<S, undefined, C>, 'schema'>,
          ) {
            super({...zeroOptions, schema});
          }
        };
      })()
    : Zero;

  const result = {
    '~': 'InitZero' as InitZeroTypes<S, C, TWrappedTransaction>,
    'Zero': TypedZero as TypedZero<S, C, boolean>,
    'defineMutator': defineMutatorWithType<S, C, TWrappedTransaction>(),
    'defineMutators': defineMutatorsWithType<S>(),
    'defineQuery': defineQueryWithType<S, C>(),
    'defineQueries': defineQueriesWithType<S>(),
  };

  return (
    options ? {...result, schema: options.schema} : result
  ) as InitZeroResult<S, C, TWrappedTransaction, boolean>;
}
