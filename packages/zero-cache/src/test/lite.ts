import {LogContext} from '@rocicorp/logger';
import {tmpdir} from 'node:os';
import {expect} from 'vitest';
import {randInt} from '../../../shared/src/rand.js';
import {Database} from '../../../zqlite/src/db.js';
import {deleteLiteDB} from '../db/delete-lite-db.js';
import {id} from '../types/sql.js';

export class DbFile {
  readonly path;

  constructor(testName: string) {
    this.path = `${tmpdir()}/${testName}-${randInt(1000000, 9999999)}.db`;
  }

  connect(lc: LogContext): Database {
    return new Database(lc, this.path);
  }

  delete() {
    deleteLiteDB(this.path);
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
      const cols = columns.map(c => id(c)).join(',');
      const vals = new Array(columns.length).fill('?').join(',');
      const insertStmt = db.prepare(
        `INSERT INTO ${id(name)} (${cols}) VALUES (${vals})`,
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
      .prepare(`SELECT * FROM ${id(table)}`)
      .safeIntegers(numberType === 'bigint')
      .all();
    expect(actual).toEqual(expect.arrayContaining(expected));
    expect(expected).toEqual(expect.arrayContaining(actual));
  }
}

export function expectMatchingObjectsInTables(
  db: Database,
  tables?: Record<string, unknown[]>,
  numberType: 'number' | 'bigint' = 'number',
) {
  for (const [table, expected] of Object.entries(tables ?? {})) {
    const actual = db
      .prepare(`SELECT * FROM ${id(table)}`)
      .safeIntegers(numberType === 'bigint')
      .all();
    expect(actual).toMatchObject(expected);
  }
}
