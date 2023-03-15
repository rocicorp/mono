import type {JSONValue, ScanNoIndexOptions} from 'replicache';
import type * as valita from 'shared/valita.js';

export type ListOptions = ScanNoIndexOptions;

/**
 * Abstract storage interface used throughout the server for storing both user
 * and system data.
 */
export interface Storage {
  put<T extends JSONValue>(key: string, value: T): Promise<void>;
  del(key: string): Promise<void>;
  get<T extends JSONValue>(
    key: string,
    schema: valita.Type<T>,
  ): Promise<T | undefined>;

  // the returned map is guaranteed to be sorted by (UTF-8) key
  list<T extends JSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>>;
}
