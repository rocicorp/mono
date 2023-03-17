import type {ReadonlyJSONValue} from 'shared/json.js';
import type * as valita from 'shared/valita.js';
import {delEntry, getEntry, listEntries, putEntry} from '../db/data.js';
import type {ListOptions, Storage} from './storage.js';

const baseAllowConcurrency = true;

// DurableObjects has a lot of clever optimizations we can take advantage of,
// but they require some thought as to whether they fit with what we are doing.
// These settings make DO behave more like a basic kv store and thus work
// better with our existing code.
// TODO: Evaluate these options and perhaps simplify our code by taking advantage.
const baseOptions = {
  // We already control currency with locks at a higher level in the game loop.
  allowConcurrency: baseAllowConcurrency,
} as const;

/**
 * Implements the Storage interface in terms of the database.
 */
export class DurableStorage implements Storage {
  private _durable: DurableObjectStorage;
  private readonly _baseOptions: Readonly<DurableObjectPutOptions>;

  constructor(durable: DurableObjectStorage, allowUnconfirmed = true) {
    this._durable = durable;
    this._baseOptions = {
      allowConcurrency: baseAllowConcurrency,
      allowUnconfirmed,
    };
  }

  put<T extends ReadonlyJSONValue>(key: string, value: T): Promise<void> {
    return putEntry(this._durable, key, value, this._baseOptions);
  }

  del(key: string): Promise<void> {
    return delEntry(this._durable, key, this._baseOptions);
  }

  get<T extends ReadonlyJSONValue>(
    key: string,
    schema: valita.Type<T>,
  ): Promise<T | undefined> {
    return getEntry(this._durable, key, schema, baseOptions);
  }

  list<T extends ReadonlyJSONValue>(
    options: ListOptions,
    schema: valita.Type<T>,
  ): Promise<Map<string, T>> {
    const doOptions = doListOptions(options);
    return listEntries(this._durable, schema, doOptions);
  }

  deleteAll(): Promise<void> {
    return this._durable.deleteAll();
  }
}

function doListOptions(opts: ListOptions): DurableObjectListOptions {
  const doOpts: DurableObjectListOptions = {
    allowConcurrency: baseAllowConcurrency,
  };

  if (opts.prefix !== undefined) {
    doOpts.prefix = opts.prefix;
  }
  if (opts.limit !== undefined) {
    doOpts.limit = opts.limit;
  }

  if (opts.start) {
    const {key, exclusive} = opts.start;
    if (exclusive) {
      doOpts.startAfter = key;
    } else {
      doOpts.start = key;
    }
  }
  return doOpts;
}
