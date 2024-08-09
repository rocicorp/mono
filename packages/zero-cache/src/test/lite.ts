import Database from 'better-sqlite3';
import {unlink} from 'fs/promises';
import {tmpdir} from 'os';
import {randInt} from 'shared/src/rand.js';

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
