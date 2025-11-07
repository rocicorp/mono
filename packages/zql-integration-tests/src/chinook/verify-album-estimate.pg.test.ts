import {beforeAll, describe, test} from 'vitest';
import {bootstrap} from '../helpers/runner.ts';
import {getChinook} from './get-deps.ts';
import {schema} from './schema.ts';

const pgContent = await getChinook();
const {dbs} = await bootstrap({
  suiteName: 'verify_album_estimate',
  pgContent,
  zqlSchema: schema,
});

describe('Verify album estimate', () => {
  beforeAll(() => {
    dbs.sqlite.exec('ANALYZE;');
  });

  test('check album title estimates', () => {
    // Get actual count
    const actual = dbs.sqlite
      .prepare("SELECT COUNT(*) as count FROM album WHERE title > 'Z'")
      .get() as {count: number};
    console.log("Actual albums with title > 'Z':", actual.count);

    // Get SQLite's estimate by preparing the query
    const stmt = dbs.sqlite.prepare(
      "SELECT * FROM album WHERE title > 'Z' ORDER BY album_id",
    );

    // Use scanstatus to get estimate
    const selectId = stmt.scanStatus(0, 1, 1); // SQLITE_SCANSTAT_SELECTID
    if (selectId !== undefined) {
      const est = stmt.scanStatus(0, 4, 1); // SQLITE_SCANSTAT_EST
      const explain = stmt.scanStatus(0, 3, 1); // SQLITE_SCANSTAT_EXPLAIN
      console.log('SQLite estimate:', est);
      console.log('Explain:', explain);
    }

    // Check if album has an index on title
    const indexes = dbs.sqlite
      .prepare(
        "SELECT * FROM sqlite_master WHERE type='index' AND tbl_name='album'",
      )
      .all();
    console.log('\nAlbum indexes:', indexes);

    // Check stat1/stat4 for album
    const stat1 = dbs.sqlite
      .prepare("SELECT * FROM sqlite_stat1 WHERE tbl='album'")
      .all();
    console.log('\nsqlite_stat1 for album:', stat1);

    try {
      const stat4 = dbs.sqlite
        .prepare("SELECT * FROM sqlite_stat4 WHERE tbl='album' LIMIT 20")
        .all();
      console.log('\nsqlite_stat4 for album (first 20):', stat4);
    } catch (e) {
      console.log('\nsqlite_stat4 not available');
    }

    // Check album titles that start with Z or later
    const albums = dbs.sqlite
      .prepare("SELECT title FROM album WHERE title > 'Z' ORDER BY title")
      .all();
    console.log('\nActual albums with title > Z:', albums);

    // Check distribution of first letters
    const distribution = dbs.sqlite
      .prepare(
        `
      SELECT SUBSTR(title, 1, 1) as first_letter, COUNT(*) as count 
      FROM album 
      GROUP BY first_letter 
      ORDER BY first_letter
    `,
      )
      .all();
    console.log('\nTitle distribution by first letter:', distribution);
  });
});
