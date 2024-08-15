import Database from 'better-sqlite3';
import {unlink} from 'fs/promises';
import {tmpdir} from 'os';
import {ident} from 'pg-format';
import {randInt} from 'shared/src/rand.js';
import {expect} from 'vitest';

export class DbFile {
  readonly path;

  constructor(testName: string) {
    this.path = `${tmpdir()}/${testName}-${randInt(10000, 99999)}.db`;
  }

  connect(): Database.Database {
    return new Database(this.path);
  }

  async unlink() {
    await unlink(this.path);
  }
}

export function initDB(
  db: Database.Database,
  statements?: string,
  tables?: Record<string, object[]>,
) {
  db.transaction(() => {
    if (statements) {
      db.exec(statements);
    }
    for (const [name, rows] of Object.entries(tables ?? {})) {
      const columns = Object.keys(rows[0]);
      const cols = columns.map(c => ident(c)).join(',');
      const vals = new Array(columns.length).fill('?').join(',');
      const insertStmt = db.prepare(
        `INSERT INTO ${ident(name)} (${cols}) VALUES (${vals})`,
      );
      for (const row of rows) {
        insertStmt.run(Object.values(row));
      }
    }
  });
}

export function expectTables(
  db: Database.Database,
  tables?: Record<string, unknown[]>,
  numberType: 'number' | 'bigint' = 'number',
) {
  for (const [table, expected] of Object.entries(tables ?? {})) {
    const actual = db
      .prepare(`SELECT * FROM ${ident(table)}`)
      .safeIntegers(numberType === 'bigint')
      .all();
    expect(actual).toEqual(expect.arrayContaining(expected));
    expect(expected).toEqual(expect.arrayContaining(actual));
  }
}
