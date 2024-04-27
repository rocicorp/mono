import type {ScanNoIndexOptions} from 'replicache';
import type * as valita from 'shared/src/valita.js';
import type {JSONValue} from '../types/bigint-json.js';

export type ListOptions = ScanNoIndexOptions;

/**
 * Abstract storage interface used throughout the server for storing both user
 * and system data.
 */
export interface Storage {
  put<T extends JSONValue>(key: string, value: T): Promise<void>;
  /**
   * Supports up to 128 key-value pairs at a time.
   */
  putEntries<T extends JSONValue>(entries: Record<string, T>): Promise<void>;
  del(key: string): Promise<void>;
  /** Supports up to 128 keys at a time. */
  delEntries(key: string[]): Promise<void>;
  get<T extends JSONValue>(
    key: string,
    schema: valita.Type<T>,
  ): Promise<T | undefined>;
  getEntries<T extends JSONValue>(
    keys: string[],
    schema: valita.Type<T>,
  ): Promise<Map<string, T>>;

  /**
   * Gets a contiguous sequence of keys and values based on the specified
   * `options`. Note that `list()` loads the entire result set into memory and
   * thus should only be used in situations in which the result is
   * guaranteed to be small (i.e. < 10 MB).
   *
   * For potentially larger result sets, use {@link scan} or {@link batchScan}.
   *
   * @returns A map of key-value results, sorted by (UTF-8) key
   */
  list<T extends JSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>>;

  /**
   * Scans a contiguous sequence of entries based on the specified `options`,
   * yielding UTF-8 key-ordered results.
   *
   * Unlike {@link list}, `scan` avoids loading an arbitrary amount of data
   * into memory and is thus recommended when processing a potentially large
   * amount of data. However, this involves multiple reads from storage
   * which may not necessarily be consistent with each other. If consistency
   * across a scan is required, the application must guarantee this with its
   * own locking scheme.
   *
   * Also see {@link batchScan} for processing batches of objects efficiently.
   */
  scan<T extends JSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): AsyncIterable<[key: string, value: T]>;

  /**
   * Scans a contiguous sequence of keys and values based on the specified
   * `options`, yielding UTF-8 ordered key results in batches of up to a
   * specified `batchSize`.
   *
   * This is similar to {@link scan} but allows the caller to
   * efficiently process larger numbers of entries as a batch.
   *
   * Similar to {@link scan}, `batchScan` involves multiple reads from storage
   * which may not necessarily be consistent with each other. If consistency
   * across a scan is required, the application must guarantee this with its
   * own locking scheme.
   */
  batchScan<T extends JSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
    batchSize: number,
  ): AsyncIterable<Map<string, T>>;
}
