import type {ScanNoIndexOptions} from 'replicache';
import type {ReadonlyJSONValue} from 'shared/json.js';
import type * as valita from 'shared/valita.js';

export type ListOptions = ScanNoIndexOptions;

/**
 * Abstract storage interface used throughout the server for storing both user
 * and system data.
 */
export interface Storage {
  put<T extends ReadonlyJSONValue>(key: string, value: T): Promise<void>;
  del(key: string): Promise<void>;
  get<T extends ReadonlyJSONValue>(
    key: string,
    schema: valita.Type<T>,
  ): Promise<T | undefined>;

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
  list<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>>;

  /**
   * Scans a contiguous sequence of entries based on the specified `options`,
   * yielding UTF-8 key-ordered results.
   *
   * Unlike {@link list}, `scan` avoids loading an arbitrary amount of data
   * into memory and is thus recommended when scanning a potentially large
   * amount of data.
   *
   * Also see {@link batchScan} for processing batches of objects efficiently.
   */
  scan<T extends ReadonlyJSONValue>(
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
   */
  batchScan<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
    batchSize: number,
  ): AsyncIterable<Map<string, T>>;
}
