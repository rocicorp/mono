/**
 * A fault in the local kv store's storage layer, as opposed to in the data it
 * holds. SQLite reports these with its own `sqlite3_errmsg` text, which every
 * driver Replicache runs on (expo-sqlite, op-sqlite, zero-sqlite) surfaces
 * verbatim inside whatever wrapper it adds, so matching the errmsg needs no
 * entry per driver.
 *
 * - `full`: `SQLITE_FULL` (13). The device is out of space (or the database
 *   hit its page limit). Every write fails until the user frees space.
 * - `cannot-open`: `SQLITE_CANTOPEN` (14). The database file cannot be
 *   opened: a missing directory, a permissions or file-protection state, a
 *   filesystem that has gone away.
 * - `io-error`: `SQLITE_IOERR` (10). A read or write to the file failed.
 *
 * None of them is corruption of the data, and none is cleared by dropping
 * the database and rebuilding: the rebuild opens the same failing storage.
 */
export type StorageFailureKind = 'full' | 'cannot-open' | 'io-error';

export type StorageFailure = {
  readonly kind: StorageFailureKind;
  /** The error the store threw, as received. */
  readonly error: unknown;
};

const SIGNATURES: readonly (readonly [RegExp, StorageFailureKind])[] = [
  [/database or disk is full|SQLITE_FULL/, 'full'],
  [/unable to open database file|SQLITE_CANTOPEN/, 'cannot-open'],
  [/disk I\/O error|SQLITE_IOERR/, 'io-error'],
];

/**
 * Classifies an error thrown by the kv store as a storage failure, or
 * `undefined` when it is not one. Looks at the error's own rendering and
 * follows its `cause` chain, since a failed transaction is reported wrapped:
 * `Transaction operation failed and release also failed: operation error =
 * <the SQLite error>; release error = ...`.
 */
export function classifyStorageFailure(
  error: unknown,
): StorageFailureKind | undefined {
  let current: unknown = error;
  // A bounded walk: a cause chain is short, and a cycle must not hang this.
  for (
    let depth = 0;
    depth < 8 && current !== undefined && current !== null;
    depth++
  ) {
    const rendered = renderError(current);
    for (const [signature, kind] of SIGNATURES) {
      if (signature.test(rendered)) {
        return kind;
      }
    }
    current = current instanceof Error ? current.cause : undefined;
  }
  return undefined;
}

function renderError(error: unknown): string {
  if (error instanceof Error) {
    return `${error.name}: ${error.message}`;
  }
  return typeof error === 'string' ? error : '';
}
