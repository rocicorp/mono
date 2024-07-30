import type Database from 'better-sqlite3';

export const queries = {
  setCommittedLsn: /*sql*/ `INSERT OR REPLACE INTO _zero_metadata (key, value) VALUES ('committed-lsn', ?);`,
  getCommittedLsn: /*sql*/ `SELECT value FROM _zero_metadata WHERE key = 'committed-lsn';`,
};
export function ensureSchema(db: Database.Database) {
  db.exec(/*sql*/ `CREATE TABLE IF NOT EXISTS _zero_metadata (
      key TEXT PRIMARY KEY,
      value TEXT
    );`);
}
