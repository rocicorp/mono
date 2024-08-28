import {LogContext} from '@rocicorp/logger';
import Database from 'better-sqlite3';
import {JSONValue} from 'shared/src/json.js';
import {Storage} from 'zql/src/zql/ivm2/operator.js';
import {Stream} from 'zql/src/zql/ivm2/stream.js';

export interface ClientGroupStorage {
  /** Creates a {@link Storage} instance for a single operator. */
  createStorage(): Storage;

  /** Deletes all storage for the client group. */
  destroy(): void;
}

type Statements = {
  get: Database.Statement;
  set: Database.Statement;
  del: Database.Statement;
  scan: Database.Statement;
  clear: Database.Statement;
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

export class DatabaseStorage {
  static create(lc: LogContext, path: string) {
    // SQLite is used for ephemeral storage (i.e. similar to RAM) that can spill to
    // disk to avoid consuming too much memory. Each worker thread gets its own
    // database (file) and acts as the single reader/writer of the DB, so
    // `locking_mode` is set to `EXCLUSIVE` for performance. Similarly, since
    // durability is not important, `synchronous` is set to `OFF` for performance.
    const db = new Database(path);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = OFF');
    db.pragma('locking_mode = EXCLUSIVE');

    db.prepare(CREATE_STORAGE_TABLE).run();
    lc.info?.(`Created DatabaseStorage backed by ${path}`);
    return new DatabaseStorage(db);
  }

  readonly #stmts: Statements;

  constructor(db: Database.Database) {
    this.#stmts = {
      get: db.prepare(`
        SELECT val FROM storage WHERE
          clientGroupID = ? AND op = ? AND key = ?
      `),
      set: db.prepare(`
        INSERT INTO storage (clientGroupID, op, key, val)
          VALUES(?, ?, ?, ?)
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
    };
  }

  #get(
    cgID: string,
    opID: number,
    key: string,
    def?: JSONValue,
  ): JSONValue | undefined {
    const {val} = this.#stmts.get.get(cgID, opID, key);
    return val ? JSON.parse(val) : def;
  }

  #set(cgID: string, opID: number, key: string, val: JSONValue) {
    this.#stmts.set.run(cgID, opID, key, JSON.stringify(val));
  }

  #del(cgID: string, opID: number, key: string) {
    this.#stmts.del.run(cgID, opID, key);
  }

  *#scan(
    cgID: string,
    opID: number,
    opts: {prefix: string} = {prefix: ''},
  ): Stream<[string, JSONValue]> {
    const {prefix} = opts;
    for (const {key, val} of this.#stmts.scan.iterate(cgID, opID, prefix)) {
      if (!key.startsWith(prefix)) {
        return;
      }
      yield [key, JSON.parse(val)];
    }
  }

  createClientGroupStorage(cgID: string): ClientGroupStorage {
    const destroy = () => this.#stmts.clear.run(cgID);
    destroy();

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
