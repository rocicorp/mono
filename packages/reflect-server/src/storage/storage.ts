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
   * For potentially larger result sets, use {@link scan}.
   *
   * @returns A map of key-value results, sorted by (UTF-8) key
   */
  list<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>>;

  /**
   * Scans a contiguous sequence of keys and values based on the specified
   * `options`, yielding UTF-8 ordered key results in batches of a `safeBatchSize`.
   *
   * `safeBatchSize` is used as a hint for a reasonable number of results to fetch
   * in each batch. If unspecified, the implementation will choose a reasonable default.
   * Because of layered Storage implementations, the actual size of batches may be more
   * or less than `safeBatchSize`.
   */
  scan<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
    safeBatchSize?: number,
  ): AsyncIterable<Map<string, T>>;
}
