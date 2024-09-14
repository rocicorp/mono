import {Database} from 'zqlite/src/db.js';
import {unlink} from 'fs/promises';
import {tmpdir} from 'os';
import {ident} from 'pg-format';
import {randInt} from 'shared/src/rand.js';
import {expect} from 'vitest';
import {LogContext} from '@rocicorp/logger';

export class DbFile {
  readonly path;

  constructor(testName: string) {
    this.path = `${tmpdir()}/${testName}-${randInt(1000000, 9999999)}.db`;
  }

  connect(lc: LogContext): Database {
    return new Database(lc, this.path);
  }

  async unlink() {
    await unlink(this.path);
  }
}

export function initDB(
  db: Database,
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
  db: Database,
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
