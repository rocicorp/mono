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

  // the returned map is guaranteed to be sorted by (UTF-8) key
  list<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>>;
}
