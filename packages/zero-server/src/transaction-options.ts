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
   *
   * A mutator that reads a row and then writes a value derived from that read
   * needs `repeatable read` or higher to be correct. At `read committed` two
   * concurrent runs can both read the old value and both write, neither one
   * raising an error, and one of the writes is silently lost. The stricter
   * levels report that case as a serialization failure, which can be retried.
   *
   * The level is set on each transaction's `BEGIN`, so it applies to mutator
   * transactions only and nothing is left on the connection afterwards. A
   * session-level `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL`
   * on the pool's connections gets the same level, but behind a
   * transaction-mode pooler (PgBouncer, Supavisor) the server connection is
   * shared, and the setting stays on it for every other client that is later
   * given that connection.
   */
  readonly isolationLevel?: IsolationLevel | undefined;
};

/**
 * `BEGIN`, carrying an isolation level when one was asked for.
 *
 * The level is a closed union rather than free text, so it is never a source
 * of injection.
 */
export function beginStatement(
  isolationLevel: IsolationLevel | undefined,
): string {
  return isolationLevel === undefined
    ? 'BEGIN'
    : `BEGIN ISOLATION LEVEL ${isolationLevel.toUpperCase()}`;
}
