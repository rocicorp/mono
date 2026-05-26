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

  #get(
    cgID: string,
    opID: number,
    key: string,
    def?: JSONValue,
  ): JSONValue | undefined {
    this.#maybeCheckpoint();
    const row = this.#stmts.get.get<{val: string}>(cgID, opID, key);
    return row ? JSON.parse(row.val) : def;
  }

  #set(cgID: string, opID: number, key: string, val: JSONValue) {
    this.#maybeCheckpoint();
    this.#stmts.set.run(cgID, opID, key, JSON.stringify(val));
  }

  #del(cgID: string, opID: number, key: string) {
    this.#maybeCheckpoint();
    this.#stmts.del.run(cgID, opID, key);
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
    const destroy = () => {
      this.#stmts.clear.run(cgID);
      this.#checkpoint();
      this.#db.compact(this.#options.compactionThresholdBytes);
    };
    this.#stmts.clear.run(cgID);

    let nextOpID = 1;
    return {
      createStorage: () => {
        const opID = nextOpID++;
        return {
          get: (key, def?) => this.#get(cgID, opID, key, def),
          set: (key, val) => this.#set(cgID, opID, key, val),
          del: key => this.#del(cgID, opID, key),
          scan: opts => this.#scan(cgID, opID, opts),
        };
      },

      destroy,
    };
  }
}
