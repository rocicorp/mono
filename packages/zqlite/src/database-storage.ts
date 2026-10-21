import type {LogContext} from '@rocicorp/logger';
import type {JSONValue} from '../../shared/src/json.ts';
import {getSizeOfEntry} from '../../shared/src/size-of-value.ts';
import {compareValues} from '../../zql/src/ivm/data.ts';
import type {Storage} from '../../zql/src/ivm/operator.ts';
import type {Stream} from '../../zql/src/ivm/stream.ts';
import type {Statement} from './db.ts';
import {Database} from './db.ts';

export interface ClientGroupStorage {
  /** Creates a {@link Storage} instance for a single operator. */
  createStorage(): Storage;

  /** Deletes all storage for the client group. */
  destroy(): void;
}

type Statements = {
  get: Statement;
  set: Statement;
  del: Statement;
  scan: Statement;
  clear: Statement;
  clearOp: Statement;
  commit: Statement;
  begin: Statement;
};

// Exported for testing.
export const CREATE_STORAGE_TABLE = `
  CREATE TABLE storage (
    clientGroupID TEXT,
    op NUMBER,
    key TEXT,
    val TEXT,
    PRIMARY KEY(clientGroupID, op, key)
  )
  `;

/**
 * The most values the storage of a spilled operator keeps in memory. Profiles
 * of the view-syncer gain as much from 64 as from 1,024.
 */
const MAX_CACHED_ENTRIES_PER_OP = 64;

/**
 * When the operators' values go over `maxMemoryBytes`, operators are spilled
 * until they are back under this proportion of it, so that spilling, which
 * sorts the operators by last use, happens in batches.
 */
const SPILL_TARGET_PROPORTION = 0.9;

/**
 * The heap bytes that operator state takes per byte of {@link getSizeOfEntry}.
 * Measured with `heapUsed` on view-syncer operator state, mostly Cap's
 * `{size, pks}` and Take's `{size, bound}`.
 */
const HEAP_BYTES_PER_SIZE = 2.6;

export type DatabaseStorageOptions = {
  commitInterval?: number | undefined;
  compactionThresholdBytes?: number | undefined;
  /**
   * The most heap bytes, as estimated from {@link getSizeOfEntry}, that the
   * operators of all client groups keep in memory before the least recently
   * used ones spill to SQLite. 0, the default, keeps every operator's values
   * in SQLite.
   */
  maxMemoryBytes?: number | undefined;
};

const defaultOptions = {
  commitInterval: 5_000,
  compactionThresholdBytes: 50 * 1024 * 1024,
  maxMemoryBytes: 0,
};

/**
 * The storage of one operator. Its values live in `values` until the
 * operator is spilled, and in SQLite afterwards, with the most recently used
 * of them in `cache`.
 */
class OpStorage {
  readonly cgID: string;
  readonly opID: number;
  /** `undefined` once spilled. */
  values: Map<string, JSONValue> | undefined;
  /** The {@link getSizeOfEntry} of `values`. */
  size = 0;
  lastUsed = 0;
  /**
   * Once spilled, the operator's most recently read and written values,
   * `undefined` for a key known to be absent. Operators read the same key
   * again and again (Cap and Take read their state on every fetch and push),
   * so this saves most of the SQLite reads.
   */
  readonly cache = new Map<string, JSONValue | undefined>();

  constructor(cgID: string, opID: number, inMemory: boolean) {
    this.cgID = cgID;
    this.opID = opID;
    this.values = inMemory ? new Map() : undefined;
  }

  cacheSet(key: string, val: JSONValue | undefined) {
    const {cache} = this;
    if (cache.size >= MAX_CACHED_ENTRIES_PER_OP && !cache.has(key)) {
      cache.clear();
    }
    cache.set(key, val);
  }
}

/**
 * Operator storage for the client groups of a sync worker.
 *
 * Each operator keeps its values in a `Map`, so reading a key that was never
 * written does not ask SQLite. When the values of all of the operators go
 * over `maxMemoryBytes`, the least recently used operators spill: their
 * values are written to SQLite, where they stay until the operator is
 * destroyed.
 *
 * Like `MemoryStorage`, `get` returns the stored value itself, which
 * operators do not mutate.
 */
export class DatabaseStorage {
  static create(
    lc: LogContext,
    path: string,
    options: DatabaseStorageOptions = {},
  ): DatabaseStorage {
    // SQLite is used for ephemeral storage (i.e. similar to RAM) that can spill to
    // disk to avoid consuming too much memory. Each worker thread gets its own
    // database (file) and acts as the single reader/writer of the DB, so
    // `locking_mode` is set to `EXCLUSIVE` for performance. Similarly, since
    // durability is not important, `synchronous` is set to `OFF` for performance.
    const db = new Database(lc, path);
    db.unsafeMode(true); // Allows journal_mode = OFF
    db.pragma('locking_mode = EXCLUSIVE');
    db.pragma('foreign_keys = OFF');
    db.pragma('journal_mode = OFF');
    db.pragma('synchronous = OFF');
    db.pragma('auto_vacuum = INCREMENTAL');

    db.prepare(CREATE_STORAGE_TABLE).run();
    lc.debug?.(`Created DatabaseStorage backed by ${path}`);
    return new DatabaseStorage(db, options);
  }

  readonly #stmts: Statements;
  readonly #options: typeof defaultOptions;
  readonly #db: Database;
  #numWrites = 0;

  /** The operators whose values are in memory. */
  readonly #inMemory = new Set<OpStorage>();
  /** The {@link getSizeOfEntry} of the values of the operators in `#inMemory`. */
  #size = 0;
  /** `maxMemoryBytes` in units of {@link getSizeOfEntry}. */
  readonly #maxSize: number;
  /** Orders the operators by last use. */
  #clock = 0;

  constructor(db: Database, options: DatabaseStorageOptions = {}) {
    this.#stmts = {
      get: db
        .prepare(
          `
        SELECT val FROM storage WHERE
          clientGroupID = ? AND op = ? AND key = ?
      `,
        )
        .pluck(true),
      set: db.prepare(`
        INSERT INTO storage (clientGroupID, op, key, val)
          VALUES(?, ?, ?, ?)
        ON CONFLICT(clientGroupID, op, key) 
        DO 
          UPDATE SET val = excluded.val
      `),
      del: db.prepare(`
        DELETE FROM storage WHERE
          clientGroupID = ? AND op = ? AND key = ?
      `),
      scan: db
        .prepare(
          `
        SELECT key, val FROM storage WHERE
          clientGroupID = ? AND op = ? AND key >= ?
      `,
        )
        .raw(true),
      clear: db.prepare(`
        DELETE FROM storage WHERE clientGroupID = ?
      `),
      clearOp: db.prepare(`
        DELETE FROM storage WHERE clientGroupID = ? AND op = ?
      `),
      commit: db.prepare('COMMIT'),
      begin: db.prepare('BEGIN'),
    };
    this.#stmts.begin.run();
    this.#options = {
      commitInterval: options.commitInterval ?? defaultOptions.commitInterval,
      compactionThresholdBytes:
        options.compactionThresholdBytes ??
        defaultOptions.compactionThresholdBytes,
      maxMemoryBytes: options.maxMemoryBytes ?? defaultOptions.maxMemoryBytes,
    };
    this.#maxSize = this.#options.maxMemoryBytes / HEAP_BYTES_PER_SIZE;
    this.#db = db;
  }

  close() {
    this.#checkpoint();
    this.#db.close();
  }

  /** The estimated size of the values that operators keep in memory. */
  get memoryBytes(): number {
    return Math.round(this.#size * HEAP_BYTES_PER_SIZE);
  }

  #get(op: OpStorage, key: string, def?: JSONValue): JSONValue | undefined {
    op.lastUsed = ++this.#clock;
    const {values} = op;
    if (values) {
      const val = values.get(key);
      return val === undefined ? def : val;
    }
    let val = op.cache.get(key);
    if (val === undefined && !op.cache.has(key)) {
      this.#maybeCheckpoint();
      const json = this.#stmts.get.get<string | undefined>(
        op.cgID,
        op.opID,
        key,
      );
      val = json === undefined ? undefined : JSON.parse(json);
      op.cacheSet(key, val);
    }
    return val === undefined ? def : val;
  }

  #set(op: OpStorage, key: string, val: JSONValue) {
    op.lastUsed = ++this.#clock;
    const {values} = op;
    if (values) {
      if (values.size === 0) {
        // The operator may have been released by destroy().
        this.#inMemory.add(op);
      }
      const old = values.get(key);
      const delta =
        getSizeOfEntry(key, val) -
        (old === undefined ? 0 : getSizeOfEntry(key, old));
      values.set(key, val);
      op.size += delta;
      this.#size += delta;
      if (this.#size > this.#maxSize) {
        this.#spillLeastRecentlyUsed();
      }
      return;
    }
    this.#write(op, key, val);
    // The cache is updated only once the database is, so a failed write
    // leaves it consistent with the database.
    op.cacheSet(key, val);
  }

  #del(op: OpStorage, key: string) {
    op.lastUsed = ++this.#clock;
    const {values} = op;
    if (values) {
      const old = values.get(key);
      if (old !== undefined) {
        values.delete(key);
        const size = getSizeOfEntry(key, old);
        op.size -= size;
        this.#size -= size;
      }
      return;
    }
    this.#maybeCheckpoint();
    this.#stmts.del.run(op.cgID, op.opID, key);
    op.cacheSet(key, undefined);
  }

  #write(op: OpStorage, key: string, val: JSONValue) {
    this.#maybeCheckpoint();
    this.#stmts.set.run(op.cgID, op.opID, key, JSON.stringify(val));
  }

  /**
   * Spills the least recently used operators until the values in memory are
   * under {@link SPILL_TARGET_PROPORTION} of the budget.
   */
  #spillLeastRecentlyUsed() {
    const target = this.#maxSize * SPILL_TARGET_PROPORTION;
    const ops: OpStorage[] = [];
    for (const op of this.#inMemory) {
      ops.push(op);
    }
    ops.sort((a, b) => a.lastUsed - b.lastUsed);
    for (const op of ops) {
      if (this.#size <= target) {
        break;
      }
      this.#spill(op);
    }
  }

  #spill(op: OpStorage) {
    const {values} = op;
    if (!values) {
      return;
    }
    for (const [key, val] of values) {
      this.#write(op, key, val);
    }
    this.#release(op);
    op.values = undefined;
  }

  /** Drops the values that `op` keeps in memory. */
  #release(op: OpStorage) {
    this.#size -= op.size;
    op.size = 0;
    op.values?.clear();
    op.cache.clear();
    this.#inMemory.delete(op);
  }

  #destroy(op: OpStorage) {
    if (!op.values) {
      this.#maybeCheckpoint();
      this.#stmts.clearOp.run(op.cgID, op.opID);
    }
    this.#release(op);
  }

  /**
   * We don't need to commit every single write to the DB
   * since we're not concerned with durability.
   * Waiting on commits can be expensive, so we commit
   * every `COMMIT_INTERVAL` writes.
   */
  #maybeCheckpoint() {
    if (++this.#numWrites >= this.#options.commitInterval) {
      this.#checkpoint();
    }
  }

  #checkpoint() {
    this.#stmts.commit.run();
    this.#stmts.begin.run();
    this.#numWrites = 0;
  }

  *#scan(
    op: OpStorage,
    opts: {prefix: string} = {prefix: ''},
  ): Stream<[string, JSONValue]> {
    const {prefix} = opts;
    const {values} = op;
    if (values) {
      const keys: string[] = [];
      for (const key of values.keys()) {
        if (key.startsWith(prefix)) {
          keys.push(key);
        }
      }
      // Sorted like SQLite's BINARY collation, which compares UTF-8 bytes.
      keys.sort(compareValues);
      for (const key of keys) {
        // Read through #get, which skips keys deleted since and still works
        // if the operator spills while the scan is suspended.
        const val = this.#get(op, key);
        if (val !== undefined) {
          yield [key, val];
        }
      }
      return;
    }
    for (const [key, val] of this.#stmts.scan.iterate<[string, string]>(
      op.cgID,
      op.opID,
      prefix,
    )) {
      if (!key.startsWith(prefix)) {
        return;
      }
      yield [key, JSON.parse(val)];
    }
  }

  createClientGroupStorage(cgID: string): ClientGroupStorage {
    // The group's live operator storages.
    const ops = new Set<OpStorage>();
    const destroy = () => {
      this.#stmts.clear.run(cgID);
      for (const op of ops) {
        this.#release(op);
      }
      this.#checkpoint();
      this.#db.compact(this.#options.compactionThresholdBytes);
    };
    this.#stmts.clear.run(cgID);

    let nextOpID = 1;
    return {
      createStorage: () => {
        const op = new OpStorage(
          cgID,
          nextOpID++,
          this.#options.maxMemoryBytes > 0,
        );
        ops.add(op);
        return {
          get: (key, def?) => this.#get(op, key, def),
          set: (key, val) => this.#set(op, key, val),
          del: key => this.#del(op, key),
          scan: opts => this.#scan(op, opts),
          destroy: () => {
            this.#destroy(op);
            ops.delete(op);
          },
        };
      },

      destroy,
    };
  }
}
