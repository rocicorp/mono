import {jsonSchema, Patch, Version} from 'reflect-protocol';
import type {AuthData, WriteTransaction} from 'reflect-types/src/mod.js';
import {
  isScanIndexOptions,
  makeScanResult,
  ScanNoIndexOptions,
  ScanOptions,
  TransactionEnvironment,
  TransactionReason,
} from 'replicache';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import * as v from 'shared/src/valita.js';
import type {ClientID} from '../types/client-state.js';
import {
  UserValue,
  userValueKey,
  userValuePrefix,
  userValueSchema,
  userValueVersionEntry,
} from '../types/user-value.js';
import type {Storage} from './storage.js';

/**
 * Implements Replicache's WriteTransaction in terms of EntryCache.
 */
export class ReplicacheTransaction implements WriteTransaction {
  readonly clientID: ClientID;
  readonly mutationID: number;
  readonly auth?: AuthData | undefined;
  #storage: Storage;
  #version: Version;

  readonly reason: TransactionReason = 'authoritative';
  readonly environment: TransactionEnvironment = 'server';

  constructor(
    storage: Storage,
    clientID: string,
    mutationID: number,
    version: Version,
    auth: AuthData | undefined,
  ) {
    this.#storage = storage;
    this.clientID = clientID;
    this.#version = version;
    this.mutationID = mutationID;
    this.auth = auth;
  }

  async put(key: string, value: ReadonlyJSONValue): Promise<void> {
    const prev = await this.#getUserValueEntry(key);
    const userValue: UserValue = {
      deleted: false,
      version: this.#version,
      value: v.parse(value, jsonSchema),
    };
    await this.#replaceUserValueEntry(key, userValue, prev);
  }

  async del(key: string): Promise<boolean> {
    const prev = await this.#getUserValueEntry(key);
    if (prev === undefined || prev.deleted) {
      return false;
    }

    // Implement del with soft delete so we can detect deletes for diff.
    const userValue: UserValue = {
      deleted: true,
      version: this.#version,
      value: prev.value, // prev came from get which needs to be verified when it was written.
    };
    await this.#replaceUserValueEntry(key, userValue, prev);
    return true;
  }

  async #replaceUserValueEntry(
    userKey: string,
    newValue: UserValue,
    prevValue: UserValue | undefined,
  ): Promise<void> {
    if (prevValue) {
      const oldIndexEntry = userValueVersionEntry(userKey, prevValue);
      // Note: Purposely do not `await` this del(), in order to ensure that it is
      // batched with the following putEntries().
      void this.#storage.del(oldIndexEntry.key);
    }

    const newIndexEntry = userValueVersionEntry(userKey, newValue);
    await this.#storage.putEntries({
      [userValueKey(userKey)]: newValue,
      [newIndexEntry.key]: newIndexEntry.value,
    });
  }

  #getUserValueEntry(key: string): Promise<UserValue | undefined> {
    return this.#storage.get(userValueKey(key), userValueSchema);
  }

  async get(key: string): Promise<ReadonlyJSONValue | undefined> {
    const entry = await this.#getUserValueEntry(key);
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

  scan(options: ScanOptions = {}) {
    if (isScanIndexOptions(options)) {
      throw new Error('not implemented');
    }

    return makeScanResult<ScanNoIndexOptions>(options, () =>
      this.#scan(options),
    );
  }

  async *#scan(options: ScanNoIndexOptions) {
    const {prefix, start} = options;

    const optsInternal = {
      ...options,
      // We cannot use the limit option because we soft-delete entries,
      // so we grab all entries and let makeScanResult() implement the limit.
      limit: undefined,
      prefix: userValueKey(prefix || ''),
      start: start && {key: userValueKey(start.key)}, // remove exclusive option, as makeScanResult will take care of it
    };

    for await (const [k, v] of this.#storage.scan(
      optsInternal,
      userValueSchema,
    )) {
      if (!v.deleted) {
        const entry: [string, ReadonlyJSONValue] = [stripPrefix(k), v.value];
        yield entry;
      }
    }
  }
}

function stripPrefix(key: string) {
  return key.slice(userValuePrefix.length);
}

export function unwrapPatch(inner: Patch): Patch {
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
