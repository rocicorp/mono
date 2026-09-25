/**
 * A fault in the local kv store's storage layer, as opposed to in the data it
 * holds.
 *
 * - `full`: the device is out of space (SQLite's `SQLITE_FULL`, WebKit's
 *   IndexedDB reporting `database or disk is full`). Every write fails until
 *   the user frees space.
 * - `cannot-open`: the database cannot be opened (SQLite's `SQLITE_CANTOPEN`:
 *   a missing directory, a permissions or file-protection state, a filesystem
 *   that has gone away; or `indexedDB.open` failing).
 * - `io-error`: a read or write to the file failed (SQLite's `SQLITE_IOERR`).
 *
 * None of them is by itself evidence that the data is corrupt, and dropping
 * the database and rebuilding does not clear `cannot-open` or `io-error`: the
 * rebuild opens the same failing storage. A write torn by a `full` disk can
 * corrupt the data, though, and dropping the database is also what frees the
 * space, so an invalid ref count under `full` still drops it.
 */
export type StorageFailureKind = 'full' | 'cannot-open' | 'io-error';

/**
 * Thrown by a kv store when its storage has failed rather than its data. The
 * SQLite stores and the IndexedDB store throw it, with the driver's or the
 * browser's error as `cause`; a custom `StoreProvider` can throw it too to get
 * the same handling (see `onStorageFailure`).
 */
export class StorageFailureError extends Error {
  name = 'StorageFailureError';
  readonly kind: StorageFailureKind;

  constructor(
    kind: StorageFailureKind,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.kind = kind;
  }
}

/**
 * The {@link StorageFailureError} in `error` or its `cause` chain. A failed
 * transaction whose release also failed is reported wrapped, with the
 * operation's error as `cause`.
 */
export function getStorageFailure(
  error: unknown,
): StorageFailureError | undefined {
  let current = error;
  while (current instanceof Error) {
    if (current instanceof StorageFailureError) {
      return current;
    }
    current = current.cause;
  }
  return undefined;
}
