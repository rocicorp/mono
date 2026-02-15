import type {Expand} from '../../shared/src/expand.ts';
import type {Schema} from './schema.ts';

/**
 * Applications can augment this interface to register their Zero types via
 * declaration merging:
 *
 * ```ts
 * declare module '@rocicorp/zero' {
 *   interface DefaultTypes {
 *     schema: typeof schema;
 *     context: AuthData | undefined;
 *     dbProvider: typeof dbProvider;
 *   }
 * }
 * ```
 */
export interface DefaultTypes {}

export type DefaultSchema<TDefaultTypes = DefaultTypes> =
  TDefaultTypes extends {
    readonly schema: infer S extends Schema;
  }
    ? S
    : Schema;

export type BaseDefaultSchema<TDefaultTypes = DefaultTypes> =
  DefaultSchema<TDefaultTypes>;

export type DefaultContext<TDefaultTypes = DefaultTypes> =
  TDefaultTypes extends {
    readonly context: infer C;
  }
    ? Expand<Readonly<C>>
    : unknown;

export type BaseDefaultContext<TDefaultTypes = DefaultTypes> =
  unknown extends DefaultContext<TDefaultTypes>
    ? // oxlint-disable-next-line no-explicit-any
      any
    : DefaultContext<TDefaultTypes>;

export type InferTransactionFromDbProvider<TDbProvider> = TDbProvider extends {
  transaction: <R>(
    // oxlint-disable-next-line no-explicit-any
    callback: (tx: infer TTransaction, ...args: any[]) => any,
    // oxlint-disable-next-line no-explicit-any
    ...args: any[]
  ) => Promise<R>;
}
  ? TTransaction
  : unknown;

export type DefaultWrappedTransaction<TDefaultTypes = DefaultTypes> =
  TDefaultTypes extends {
    readonly dbProvider: infer DbProvider;
  }
    ? InferTransactionFromDbProvider<DbProvider> extends infer TTransaction
      ? TTransaction extends {
          readonly dbTransaction: {
            readonly wrappedTransaction: infer TWrappedTransaction;
          };
        }
        ? TWrappedTransaction
        : {
            error: `The \`dbProvider\` type you have registered with \`declare module '@rocicorp/zero'\` is incorrect.`;
            registeredDbProvider: DbProvider;
          }
      : never
    : unknown;
