import type {Enum} from '../../../shared/src/enum.ts';
import {type PostgresDB, type PostgresTransaction} from '../types/pg.ts';
import type * as Mode from './mode-enum.ts';
import {READ_COMMITTED} from './mode-enum.ts';

type Mode = Enum<typeof Mode>;

// The default timeout is settable by ZERO_IDLE_IN_TRANSACTION_SESSION_TIMEOUT
// as an emergency measure and is explicitly not made available as a server
// option. This value is function of how the zero-cache uses transactions, and
// should never need to be "tuned" or adjusted for different environments.
const DEFAULT_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS = parseInt(
  process.env.ZERO_IDLE_IN_TRANSACTION_SESSION_TIMEOUT ?? '60000',
);

export type TransactionOptions = {
  mode?: Mode;
  idleInTransactionTimeoutMs?: number;
};

/**
 * Runs a zero-cache transaction on the given postgres DB. For consistency
 * across various postgres providers, certain transaction-level parameters
 * are set to override any defaults set by the provider, including:
 * * `statement_timeout` (disabled)
 * * `idle_in_transaction_session_timeout` (1min)
 */
export function runTx<T>(
  db: PostgresDB,
  fn: (tx: PostgresTransaction) => T | Promise<T>,
  opts: TransactionOptions = {},
) {
  const {
    // Explicitly default to the Postgres default to override any custom
    // `default_transaction_isolation`.
    mode = READ_COMMITTED,
    idleInTransactionTimeoutMs = DEFAULT_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS,
  } = opts;
  return db.begin(mode, sql => {
    // Disable any statement_timeout for the current transaction. By default,
    // Postgres does not impose a statement timeout, but some users and
    // providers set one at the database level (even though it is explicitly
    // discouraged by the Postgres documentation).
    //
    // Zero logic in particular often does not fit into the category of general
    // application logic; for potentially long-running operations like
    // migrations and background cleanup, the statement timeout should be
    // disabled to prevent these operations from timing out.
    void sql`SET LOCAL statement_timeout = 0;`.execute();

    // Set an idle_in_transaction_session_timeout to limit the blast radius of
    // orphaned transactions. The zero-cache does not keep transactions open
    // and idle (though this may change if support for streaming of in-progress
    // transactions is added to our logical replication layer).
    void sql
      .unsafe(
        /*sql*/ `SET LOCAL idle_in_transaction_session_timeout = ${idleInTransactionTimeoutMs}`,
      )
      .execute();

    return fn(sql);
  });
}
