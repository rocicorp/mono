import {compareUTF8} from 'compare-utf8';

export type DurableObjectListOptions = {
  allowConcurrency?: boolean | undefined;
  /** Only include keys starting with `prefix`. */
  prefix?: string | undefined;
  /** Only include up to `limit` results. */
  limit?: number | undefined;
  /** When provided the scan starts at this key. */
  start?:
    | {
        key: string;
        /** Whether the `key` is exclusive or inclusive. */
        exclusive?: boolean | undefined;
      }
    | undefined;

  /** Exclusive end */
  end?: string | undefined;
  reverse?: boolean | undefined;
  startAfter?: string | undefined;
};

export function runWithFakeDurableObjectStorage<R>(
  fn: (storage: FakeDurableObjectStorage) => R | Promise<R>,
): Promise<R> {
  return Promise.resolve(fn(new FakeDurableObjectStorage()));
}

export class FakeDurableObjectStorage {
  readonly #entries = new Map<string, unknown>();

  get<T = unknown>(key: string): Promise<T | undefined>;
  get<T = unknown>(keys: string[]): Promise<Map<string, T>>;
  // eslint-disable-next-line require-await
  async get<T = unknown>(keys: string | string[]) {
    if (typeof keys === 'string') {
      return this.#entries.get(keys) as T;
    }
    const entries = keys
      .filter(k => this.#entries.has(k))
      .sort(compareUTF8)
      .map(k => [k, this.#entries.get(k)] as [string, T]);
    return new Map(entries);
  }

  // eslint-disable-next-line require-await
  async list<T = unknown>(
    options: DurableObjectListOptions,
  ): Promise<Map<string, T>> {
    const {prefix, start, startAfter, end, reverse, limit} = options;
    const keys = [...this.#entries.keys()]
      .filter(
        k =>
          (!prefix || k.startsWith(prefix)) &&
          (!start || compareUTF8(start.key, k) <= 0) &&
          (!startAfter || compareUTF8(startAfter, k) < 0) &&
          (!end || compareUTF8(end, k) > 0),
      )
      .sort(compareUTF8);
    if (reverse) {
      keys.reverse();
    }
    if (limit !== undefined && keys.length > limit) {
      keys.splice(limit);
    }
    return new Map(keys.map(k => [k, this.#entries.get(k)] as [string, T]));
  }
  put<T>(key: string, value: T): Promise<void>;
  put<T>(entries: Record<string, T>): Promise<void>;
  // eslint-disable-next-line require-await
  async put<T>(keyOrEntries: string | Record<string, T>, value?: T) {
    if (typeof keyOrEntries === 'string') {
      this.#entries.set(keyOrEntries, value);
    } else {
      Object.entries(keyOrEntries).forEach(([k, v]) => this.#entries.set(k, v));
    }
  }
  delete(key: string): Promise<boolean>;
  delete(keys: string[]): Promise<number>;
  // eslint-disable-next-line require-await
  async delete(keys: string | string[]) {
    if (typeof keys === 'string') {
      return this.#entries.delete(keys);
    }
    return keys
      .map(k => this.#entries.delete(k))
      .reduce((count, deleted) => (deleted ? count + 1 : count), 0);
  }

  // eslint-disable-next-line require-await
  async deleteAll(): Promise<void> {
    this.#entries.clear();
  }

  async sync() {}

  transaction<T>(): Promise<T> {
    throw new Error('unsupported');
  }
  getAlarm(): Promise<number | null> {
    throw new Error('unsupported');
  }
  setAlarm(): Promise<void> {
    throw new Error('unsupported');
  }
  deleteAlarm(): Promise<void> {
    throw new Error('unsupported');
  }
  transactionSync<T>(): T {
    throw new Error('unsupported');
  }
}
