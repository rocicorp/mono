/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable require-await */
/* eslint-disable no-console */
import type {
  SQLiteExecuteAsyncResult,
  SQLiteBindValue,
  SQLiteOpenOptions,
  SQLiteDatabase,
} from 'expo-sqlite';
import {vi} from 'vitest';

/** Global in-memory data to simulate a DB. */
export const mockDbData: Record<string, string> = {};

/** Clear mock data between tests. */
export function clearMockDbData() {
  Object.keys(mockDbData).forEach(key => delete mockDbData[key]);
}

// Define the type for the mock transaction object passed to the callback
// In the real API, this is just the SQLiteDatabase itself, but for mocking
// we might only need a subset of methods if transaction.ts relies on them.
// Crucially, it does NOT have an explicit commit method in the real API for this context.
interface MockTxContext extends Pick<SQLiteDatabase, 'prepareAsync'> {
  // Add any other methods if ExpoSQLiteTransaction calls them on the tx object
}

//
// ---- Vitest Mock ----
//

vi.mock('expo-sqlite', () => {
  // Track active readers and writers for proper concurrency control
  let activeReaders = 0;
  let activeWriter = false;
  let writerWaiting = false;

  /** Helper to wait until a condition is met */
  const waitUntil = (condition: () => boolean): Promise<void> =>
    new Promise(resolve => {
      if (condition()) {
        resolve();
        return;
      }

      const checkInterval = setInterval(() => {
        if (condition()) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 5);
    });

  /** Wait until no readers are active */
  const waitUntilNoReaders = (): Promise<void> =>
    waitUntil(() => activeReaders === 0);

  /** Wait until no writer is active or waiting */
  const waitUntilNoWriter = (): Promise<void> =>
    waitUntil(() => !activeWriter && !writerWaiting);

  class NativeStatement {}

  class NativeDatabase {
    constructor(
      _databasePath: string,
      _options?: SQLiteOpenOptions,
      _serializedData?: Uint8Array,
    ) {
      // Implementation
    }

    async initAsync() {}
    initSync() {}
    async closeAsync() {}
    closeSync() {}
    async execAsync(sql: string) {
      // Use the mocked transaction mechanism
      await mockDb.withExclusiveTransactionAsync(async (tx: MockTxContext) => {
        const statement = await tx.prepareAsync(sql);
        await statement.executeAsync();
        await statement.finalizeAsync();
        // Successful completion implies commit in the mock's logic
      });
    }
    execSync(_sql: string) {}
    async serializeAsync(_dbName: string) {
      return new Uint8Array();
    }
    serializeSync(_dbName: string) {
      return new Uint8Array();
    }
    async isInTransactionAsync() {
      // Reflects if a write transaction is active in the mock
      return activeWriter;
    }
    isInTransactionSync() {
      // Reflects if a write transaction is active in the mock
      return activeWriter;
    }
  }

  /** Our top-level database object returned by openDatabaseAsync. */
  const mockDb = {
    /** Handles read transactions with proper concurrency */
    async readTransactionAsync(
      readCallback: (tx: Pick<SQLiteDatabase, 'prepareAsync'>) => Promise<void>,
    ) {
      // If there's a writer or a writer waiting, must queue reads
      await waitUntilNoWriter();

      // Begin read transaction
      activeReaders++;
      console.log(
        `[MockDB] readTransactionAsync START | Readers: ${activeReaders}`,
      );

      // Create snapshot of data for consistent read view
      const snapshotData = {...mockDbData};

      // Create a read-only transaction object providing prepareAsync
      const readTx: Pick<SQLiteDatabase, 'prepareAsync'> = {
        prepareAsync: vi.fn().mockImplementation(async (sql: string) => ({
          executeAsync: async (
            ...params: SQLiteBindValue[]
          ): Promise<SQLiteExecuteAsyncResult<unknown>> => {
            // Read from snapshotData
            if (sql.includes('CREATE TABLE')) {
              return mockResult([]);
            } else if (sql.includes('SELECT value FROM entry WHERE key = ?')) {
              const key = String(params[0]);
              const val = snapshotData[key];
              const rowsArray = val !== undefined ? [{value: val}] : [];
              return mockResult(rowsArray);
            } else if (sql.includes('SELECT key FROM entry WHERE key = ?')) {
              const key = String(params[0]);
              const exists = snapshotData[key] !== undefined;
              const rowsArray = exists ? [{key}] : [];
              return mockResult(rowsArray);
            } else if (
              sql.includes('DELETE FROM entry') ||
              sql.includes('INSERT OR REPLACE INTO entry')
            ) {
              throw new Error('Cannot write in read transaction');
            }
            console.warn('[MockDB] Unhandled read SQL:', sql);
            return mockResult([]);
          },
          finalizeAsync: async () => {
            // no-op
          },
        })),
      };

      let errorOccurred: unknown;
      try {
        await readCallback(readTx);
      } catch (err) {
        errorOccurred = err;
        console.error('[MockDB] Read transaction error:', err);
      } finally {
        activeReaders--;
        console.log(
          `[MockDB] readTransactionAsync END | Readers: ${activeReaders}`,
        );
        if (errorOccurred) {
          // eslint-disable-next-line no-unsafe-finally
          throw errorOccurred;
        }
      }
    },

    /** Mocks an "exclusive" transaction simulating real commit/rollback. */
    withExclusiveTransactionAsync: vi
      .fn()
      .mockImplementation(
        async (transactionCallback: (tx: MockTxContext) => Promise<void>) => {
          // Signal that a writer is waiting
          writerWaiting = true;

          // Wait for all readers to finish before acquiring write lock
          await waitUntilNoReaders();

          // Acquire write lock
          writerWaiting = false;
          activeWriter = true;

          console.log(
            `[MockDB] withExclusiveTransactionAsync START | Global State: ${JSON.stringify(
              mockDbData,
            )}`,
          );

          // Copy the global data at transaction start
          const transactionData = {...mockDbData};

          // Function to apply changes to the global mock data
          function commitChanges(updatedData: Record<string, string>) {
            clearMockDbData();
            Object.assign(mockDbData, updatedData);
          }

          // Create the mock transaction context object passed to the callback
          // It only needs the methods that ExpoSQLiteTransaction actually calls on it.
          const mockTxContext: MockTxContext = {
            prepareAsync: vi.fn().mockImplementation(async (sql: string) => ({
              executeAsync: async (
                ...params: SQLiteBindValue[]
              ): Promise<SQLiteExecuteAsyncResult<unknown>> => {
                // Manipulate the *transaction-local* data
                if (sql.includes('CREATE TABLE')) {
                  return mockResult([]);
                } else if (
                  sql.includes('SELECT value FROM entry WHERE key = ?')
                ) {
                  const key = String(params[0]);
                  const val = transactionData[key];
                  const rowsArray = val !== undefined ? [{value: val}] : [];
                  return mockResult(rowsArray);
                } else if (
                  sql.includes('SELECT key FROM entry WHERE key = ?')
                ) {
                  const key = String(params[0]);
                  const exists = transactionData[key] !== undefined;
                  const rowsArray = exists ? [{key}] : [];
                  return mockResult(rowsArray);
                } else if (sql.includes('INSERT OR REPLACE INTO entry')) {
                  const key = String(params[0]);
                  const value = String(params[1]);
                  transactionData[key] = value;
                  return mockResult([]);
                } else if (sql.includes('DELETE FROM entry WHERE key = ?')) {
                  const key = String(params[0]);
                  delete transactionData[key];
                  return mockResult([]);
                } else if (sql.includes('DELETE FROM entry')) {
                  Object.keys(transactionData).forEach(
                    k => delete transactionData[k],
                  );
                  return mockResult([]);
                }
                console.warn('[MockDB] Unhandled SQL:', sql);
                return mockResult([]);
              },
              finalizeAsync: async () => {
                // no-op
              },
            })),
            // Add other methods here if ExpoSQLiteTransaction needs them from the tx object
          };

          let errorOccurred: unknown;
          try {
            // Pass our mock transaction context to the user's callback
            await transactionCallback(mockTxContext);
          } catch (err) {
            errorOccurred = err;
            console.error(
              '[MockDB] Transaction callback error -> rolling back:',
              err,
            );
            // If an error occurs, changes in transactionData are discarded
          } finally {
            // Release write lock *before* potentially throwing
            activeWriter = false;

            // Commit changes ONLY if the callback completed without error
            if (!errorOccurred) {
              console.log(
                '[MockDB] Transaction callback succeeded -> committing changes',
              );
              commitChanges(transactionData);
            } else {
              console.log(
                '[MockDB] Transaction callback failed -> changes discarded (rollback)',
              );
            }

            console.log(
              `[MockDB] withExclusiveTransactionAsync END | Global State: ${JSON.stringify(
                mockDbData,
              )}`,
            );

            // Rethrow the original error if one occurred
            if (errorOccurred) {
              // eslint-disable-next-line no-unsafe-finally
              throw errorOccurred;
            }
          }
        },
      ),
    /** Called by your store's close() method. */
    closeAsync: vi.fn().mockResolvedValue(undefined),
    /** Called by your store's execAsync() or open logic - uses the TX mechanism. */
    execAsync: vi.fn().mockImplementation(async (sql: string) => {
      console.log('[MockDB] execAsync:', sql);
      await mockDb.withExclusiveTransactionAsync(async (tx: MockTxContext) => {
        const s = await tx.prepareAsync(sql);
        await s.executeAsync();
        await s.finalizeAsync();
        // Successful completion implies commit due to withExclusiveTransactionAsync logic
      });
    }),
  };

  /** Mocks the returned rows, etc. */
  function mockResult(rows: Record<string, SQLiteBindValue>[]) {
    const result: SQLiteExecuteAsyncResult<any> = {
      lastInsertRowId: 0,
      changes: 0,
      async getAllAsync() {
        return rows;
      },
      async getFirstAsync() {
        return rows.length > 0 ? rows[0] : null;
      },
      resetAsync: vi.fn(),
      next: vi.fn(),
      async *[Symbol.asyncIterator]() {
        for (const row of rows) {
          yield row;
        }
      },
    };
    return result;
  }

  return {
    // eslint-disable-next-line @typescript-eslint/naming-convention
    NativeDatabase,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    NativeStatement,
    ensureDatabasePathExistsAsync: vi.fn().mockResolvedValue(undefined),
    ensureDatabasePathExistsSync: vi.fn(),
    deleteDatabaseAsync: vi.fn().mockImplementation(async (dbPath: string) => {
      console.log(`[MockDB] deleteDatabaseAsync: ${dbPath}`);
      clearMockDbData();
    }),
    deleteDatabaseSync: vi.fn(),
    addListener: vi.fn().mockReturnValue({remove: vi.fn()}),
    defaultDatabaseDirectory: '/mocked/path',
    /** The database object your code receives. */
    openDatabaseAsync: vi.fn().mockResolvedValue(mockDb),
  };
});
