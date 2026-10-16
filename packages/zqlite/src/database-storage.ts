import type {LogContext} from '@rocicorp/logger';
import type {JSONValue} from '../../shared/src/json.ts';
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
 * The most values each operator's storage keeps in memory. The storage is in
 * SQLite so that operator state can spill to disk, so only a few values per
 * operator are kept: profiles of the view-syncer gain as much from 64 as from
 * 1,024.
 */
const MAX_CACHED_ENTRIES_PER_OP = 64;

const defaultOptions = {
  commitInterval: 5_000,
  compactionThresholdBytes: 50 * 1024 * 1024,
};

export class DatabaseStorage {
  static create(
    lc: LogContext,
    path: string,
    options = defaultOptions,
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

  constructor(db: Database, options = defaultOptions) {
    this.#stmts = {
      get: db.prepare(`
        SELECT val FROM storage WHERE
          clientGroupID = ? AND op = ? AND key = ?
      `),
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
      scan: db.prepare(`
        SELECT key, val FROM storage WHERE
          clientGroupID = ? AND op = ? AND key >= ?
      `),
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
    this.#options = options;
    this.#db = db;
  }

  close() {
    this.#checkpoint();
    this.#db.close();
  }

  #get(cgID: string, opID: number, key: string): JSONValue | undefined {
    this.#maybeCheckpoint();
    const row = this.#stmts.get.get<{val: string}>(cgID, opID, key);
    return row ? JSON.parse(row.val) : undefined;
  }

  #set(cgID: string, opID: number, key: string, val: JSONValue) {
    this.#maybeCheckpoint();
    this.#stmts.set.run(cgID, opID, key, JSON.stringify(val));
  }

  #del(cgID: string, opID: number, key: string) {
    this.#maybeCheckpoint();
    this.#stmts.del.run(cgID, opID, key);
  }

  #destroy(cgID: string, opID: number) {
    this.#maybeCheckpoint();
    this.#stmts.clearOp.run(cgID, opID);
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
    cgID: string,
    opID: number,
    opts: {prefix: string} = {prefix: ''},
  ): Stream<[string, JSONValue]> {
    const {prefix} = opts;
    for (const {key, val} of this.#stmts.scan.iterate<{
      key: string;
      val: string;
    }>(cgID, opID, prefix)) {
      if (!key.startsWith(prefix)) {
        return;
      }
      yield [key, JSON.parse(val)];
    }
  }

  createClientGroupStorage(cgID: string): ClientGroupStorage {
    // The caches of the group's live operator storages.
    const caches = new Set<Map<string, JSONValue | undefined>>();
    const destroy = () => {
      this.#stmts.clear.run(cgID);
      for (const cache of caches) {
        cache.clear();
      }
      this.#checkpoint();
      this.#db.compact(this.#options.compactionThresholdBytes);
    };
    this.#stmts.clear.run(cgID);

    let nextOpID = 1;
    return {
      createStorage: () => {
        const opID = nextOpID++;
        // The operator's most recently read and written values, `undefined`
        // for a key known to be absent. Operators read the same key again and
        // again (Cap and Take read their state on every fetch and push), so
        // this saves most of the SQLite reads. Like MemoryStorage, `get`
        // returns the stored value itself, which operators do not mutate.
        const cache = new Map<string, JSONValue | undefined>();
        caches.add(cache);
        const cacheSet = (key: string, val: JSONValue | undefined) => {
          if (cache.size >= MAX_CACHED_ENTRIES_PER_OP && !cache.has(key)) {
            cache.clear();
          }
          cache.set(key, val);
        };
        return {
          get: (key, def?) => {
            let val = cache.get(key);
            if (val === undefined && !cache.has(key)) {
              val = this.#get(cgID, opID, key);
              cacheSet(key, val);
            }
            return val === undefined ? def : val;
          },
          // The cache is updated only once the database is, so a failed write
          // leaves it consistent with the database.
          set: (key, val) => {
            this.#set(cgID, opID, key, val);
            cacheSet(key, val);
          },
          del: key => {
            this.#del(cgID, opID, key);
            cacheSet(key, undefined);
          },
          scan: opts => this.#scan(cgID, opID, opts),
          destroy: () => {
            this.#destroy(cgID, opID);
            cache.clear();
            caches.delete(cache);
          },
        };
      },

      destroy,
    };
  }
}
