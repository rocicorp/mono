import type {DelOp, PutOp, Version} from 'reflect-protocol';
import type {
  AuthData,
  Env,
  TransactionLocation,
  WriteTransaction,
} from 'reflect-shared/src/mod.js';
import {
  DeepReadonly,
  IndexKey,
  ScanIndexOptions,
  ScanNoIndexOptions,
  ScanOptions,
  ScanResult,
  TransactionReason,
  isScanIndexOptions,
  makeScanResult,
} from 'replicache';
import {jsonSchema} from 'shared/src/json-schema.js';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as v from 'shared/src/valita.js';
import type {ClientID} from '../types/client-state.js';
import {
  UserValue,
  userValueKey,
  userValuePrefix,
  userValueSchema,
} from '../types/user-value.js';
import type {Storage} from './storage.js';

export const NOOP_MUTATION_ID = -1;

/**
 * Implements Replicache's WriteTransaction in terms of EntryCache.
 */
export class ReplicacheTransaction implements WriteTransaction {
  readonly clientID: ClientID;
  readonly mutationID: number;
  readonly auth?: AuthData | undefined;
  readonly env: Env;
  #storage: Storage;
  #version: Version;

  readonly reason: TransactionReason = 'authoritative';
  readonly environment: TransactionLocation = 'server';
  readonly location: TransactionLocation = 'server';

  constructor(
    storage: Storage,
    clientID: string,
    mutationID: number,
    version: Version,
    auth: AuthData | undefined,
    env: Env,
  ) {
    this.#storage = storage;
    this.clientID = clientID;
    this.#version = version;
    this.mutationID = mutationID;
    this.auth = auth;
    this.env = env;
  }

  /**
   * @deprecated Use `set` instead.
   */
  put(key: string, value: ReadonlyJSONValue): Promise<void> {
    return this.set(key, value);
  }

  async set(key: string, value: ReadonlyJSONValue): Promise<void> {
    const userValue: UserValue = {
      deleted: false,
      version: this.#version,
      value: v.parse(value, jsonSchema),
    };
    await this.#storage.put(userValueKey(key), userValue);
  }

  async del(key: string): Promise<boolean> {
    const prev = await this.get(key);
    if (prev === undefined) {
      return false;
    }

    // Implement del with soft delete so we can detect deletes for diff.
    const userValue: UserValue = {
      deleted: true,
      version: this.#version,
      value: prev, // prev came from get which needs to be verified when it was written.
    };
    await this.#storage.put(userValueKey(key), userValue);
    return prev !== undefined;
  }

  async get(key: string): Promise<ReadonlyJSONValue | undefined> {
    const entry = await this.#storage.get(userValueKey(key), userValueSchema);
    if (entry === undefined) {
      return undefined;
    }
    return entry.deleted ? undefined : entry.value;
  }

  async has(key: string): Promise<boolean> {
    const val = await this.get(key);
    return val !== undefined;
  }

  async isEmpty(): Promise<boolean> {
    const sr = this.scan();
    const {done} = await sr.keys().next();
    return !!done;
  }

  scan(options: ScanIndexOptions): ScanResult<IndexKey, ReadonlyJSONValue>;
  scan(options?: ScanNoIndexOptions): ScanResult<string, ReadonlyJSONValue>;
  scan(options?: ScanOptions): ScanResult<IndexKey | string, ReadonlyJSONValue>;

  scan<V extends ReadonlyJSONValue>(
    options: ScanIndexOptions,
  ): ScanResult<IndexKey, DeepReadonly<V>>;
  scan<V extends ReadonlyJSONValue>(
    options?: ScanNoIndexOptions,
  ): ScanResult<string, DeepReadonly<V>>;
  scan<V extends ReadonlyJSONValue>(
    options?: ScanOptions,
  ): ScanResult<IndexKey | string, DeepReadonly<V>>;

  scan(
    options: ScanOptions = {},
  ): ScanResult<IndexKey | string, ReadonlyJSONValue> {
    if (isScanIndexOptions(options)) {
      throw new Error('not implemented');
    }
    return scanUserValues(this.#storage, options);
  }
}

export function scanUserValues(storage: Storage, options: ScanNoIndexOptions) {
  return makeScanResult<ScanNoIndexOptions>(options, () =>
    scanStorage(storage, options),
  );
}

async function* scanStorage(storage: Storage, options: ScanNoIndexOptions) {
  const {prefix, start} = options;

  const optsInternal = {
    ...options,
    // We cannot use the limit option because we soft-delete entries,
    // so we grab all entries and let makeScanResult() implement the limit.
    limit: undefined,
    prefix: userValueKey(prefix || ''),
    start: start && {key: userValueKey(start.key)}, // remove exclusive option, as makeScanResult will take care of it
  };

  for await (const [k, v] of storage.scan(optsInternal, userValueSchema)) {
    if (!v.deleted) {
      const entry: [string, ReadonlyJSONValue] = [stripPrefix(k), v.value];
      yield entry;
    }
  }
}

function stripPrefix(key: string) {
  return key.slice(userValuePrefix.length);
}

export function unwrapPatch(inner: (PutOp | DelOp)[]): (PutOp | DelOp)[] {
  return inner
    .filter(p => p.key.startsWith(userValuePrefix))
    .map(p => {
      const {key, op} = p;
      const unwrappedKey = stripPrefix(key);
      if (op === 'put') {
        const userValue = p.value as UserValue;
        if (userValue.deleted) {
          return {
            op: 'del',
            key: unwrappedKey,
          };
        }
        return {
          op: 'put',
          key: unwrappedKey,
          value: userValue.value,
        };
      }
      // We don't use del or clear at this layer
      throw new Error(`unexpected op: ${op}`);
    });
}
