import {afterAll, beforeAll} from 'vitest';
import {bench, describe, use} from '../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database, type Statement} from '../../../../zqlite/src/db.ts';

const ROW_COUNT = 100_000;
const PROBE_ID = ROW_COUNT / 2;

describe('snapshotter nullable unique-key lookup', () => {
  const lc = createSilentLogContext();
  let db: Database;
  let legacyLookup: Statement;
  let filteredLookup: Statement;

  beforeAll(() => {
    db = new Database(lc, ':memory:');
    db.exec(/*sql*/ `
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        handle TEXT UNIQUE,
        payload TEXT NOT NULL
      );
    `);

    const insert = db.prepare(
      'INSERT INTO users (id, handle, payload) VALUES (?, ?, ?)',
    );
    db.transaction(() => {
      for (let id = 1; id <= ROW_COUNT; id++) {
        insert.run(
          id,
          id === PROBE_ID ? null : `user-${id}`,
          'x'.repeat(128),
        );
      }
    });

    // The old `getRows` query shape. SQLite does not use MULTI-INDEX OR when
    // a bound branch contains NULL, so this does a full table scan.
    legacyLookup = db.prepare(
      'SELECT id, handle, payload FROM users WHERE id = ? OR handle = ?',
    );

    // The current `getRows` shape after it drops the nullable unique key.
    filteredLookup = db.prepare(
      'SELECT id, handle, payload FROM users WHERE id = ?',
    );
  });

  afterAll(() => {
    db.close();
  });

  bench('legacy nullable OR lookup', () => {
    use(legacyLookup.all(PROBE_ID, null));
  });

  bench('filtered unique-key lookup', () => {
    use(filteredLookup.all(PROBE_ID));
  });
});
