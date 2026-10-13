/**
 * The Postgres transaction isolation level a mutator transaction runs at.
 *
 * Spelled as Postgres spells it, so a level can be passed through to adapters
 * that take it verbatim.
 */
export type IsolationLevel =
  | 'read committed'
  | 'repeatable read'
  | 'serializable';

/**
 * Options shared by every server adapter factory.
 */
export type TransactionOptions = {
  /**
   * Isolation level for the transaction each mutation runs in.
   *
   * Left unset, the transaction is opened with a bare `BEGIN` and the database
   * decides, which is `read committed` on a stock Postgres.
   */
  readonly isolationLevel?: IsolationLevel | undefined;
};

/**
 * `BEGIN`, carrying an isolation level when one was asked for.
 */
export function beginStatement(
  isolationLevel: IsolationLevel | undefined,
): string {
  return isolationLevel === undefined
    ? 'BEGIN'
    : `BEGIN ISOLATION LEVEL ${isolationLevel.toUpperCase()}`;
}
