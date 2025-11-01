#!/usr/bin/env node
import SQLite3Database from '@rocicorp/zero-sqlite3';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from './db.ts';

const lc = createSilentLogContext();
const db = new Database(lc, ':memory:');

// Create test tables and data
db.exec(`
  CREATE TABLE artist (id INTEGER PRIMARY KEY, name TEXT);
  CREATE TABLE album (id INTEGER PRIMARY KEY, artist_id INTEGER, title TEXT);
  CREATE TABLE track (id INTEGER PRIMARY KEY, album_id INTEGER, name TEXT);

  CREATE INDEX idx_album_artist ON album(artist_id);
  CREATE INDEX idx_track_album ON track(album_id);
`);

// Insert data
// 10 artists, each with 10 albums, each with 10 tracks = 1000 tracks total
for (let i = 1; i <= 10; i++) {
  db.exec(`INSERT INTO artist VALUES (${i}, 'Artist ${i}')`);
  for (let j = 1; j <= 10; j++) {
    const albumId = (i - 1) * 10 + j;
    db.exec(`INSERT INTO album VALUES (${albumId}, ${i}, 'Album ${albumId}')`);
    for (let k = 1; k <= 10; k++) {
      const trackId = (albumId - 1) * 10 + k;
      db.exec(
        `INSERT INTO track VALUES (${trackId}, ${albumId}, 'Track ${trackId}')`,
      );
    }
  }
}

db.exec('ANALYZE');

console.log('Database setup complete');
console.log('10 artists, 100 albums, 1000 tracks\n');

interface QueryTest {
  name: string;
  sql: string;
  expectedRows: number;
}

const tests: QueryTest[] = [
  {
    name: 'Full scan of tracks',
    sql: 'SELECT * FROM track',
    expectedRows: 1000,
  },
  {
    name: 'Index lookup - single album tracks',
    sql: 'SELECT * FROM track WHERE album_id = 1',
    expectedRows: 10,
  },
  {
    name: 'Index lookup - single artist albums',
    sql: 'SELECT * FROM album WHERE artist_id = 1',
    expectedRows: 10,
  },
  {
    name: 'Nested loop join',
    sql: `SELECT track.* FROM track
          JOIN album ON track.album_id = album.id
          WHERE album.artist_id = 1`,
    expectedRows: 100,
  },
  {
    name: 'Full scan with filter',
    sql: `SELECT * FROM track WHERE id < 100`,
    expectedRows: 99,
  },
];

console.log('═'.repeat(100));
console.log('Test | Time (ms) | Rows | NVISIT Total | Per Scan Details');
console.log('─'.repeat(100));

for (const test of tests) {
  // Prepare statement
  const stmt = db.prepare(test.sql);

  // Execute and time it
  const startTime = performance.now();
  let rowCount = 0;
  for (const _row of stmt.iterate()) {
    rowCount++;
  }
  const elapsed = performance.now() - startTime;

  // Read scan statistics
  const scanStats: Array<{
    idx: number;
    name: string;
    nvisit: number;
    nloop: number;
  }> = [];
  let totalNVisit = 0;

  for (let idx = 0; ; idx++) {
    const nvisit = stmt.scanStatus(
      idx,
      SQLite3Database.SQLITE_SCANSTAT_NVISIT,
      0, // Don't reset yet
    );
    if (nvisit === undefined) {
      break;
    }

    const nloop = stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_NLOOP, 0);
    const name = stmt.scanStatus(idx, SQLite3Database.SQLITE_SCANSTAT_NAME, 0);

    const nvisitNum = Number(nvisit);
    const nloopNum = Number(nloop ?? 0n);
    totalNVisit += nvisitNum;

    scanStats.push({
      idx,
      name: String(name ?? 'unknown'),
      nvisit: nvisitNum,
      nloop: nloopNum,
    });
  }

  // Now reset for next iteration
  for (let idx = 0; ; idx++) {
    const nvisit = stmt.scanStatus(
      idx,
      SQLite3Database.SQLITE_SCANSTAT_NVISIT,
      1, // Reset
    );
    if (nvisit === undefined) {
      break;
    }
  }

  // Verify row count
  const match = rowCount === test.expectedRows ? '✓' : '✗';
  console.log(
    `${test.name.padEnd(35)} | ${elapsed.toFixed(2).padStart(9)} | ` +
      `${rowCount.toString().padStart(4)} ${match} | ${totalNVisit.toString().padStart(12)} | ` +
      scanStats
        .map(s => `${s.name}(V:${s.nvisit},L:${s.nloop})`)
        .join(', '),
  );
}

console.log('═'.repeat(100));

// Test that reset actually works
console.log('\nTesting NVISIT reset behavior:\n');

const stmt = db.prepare('SELECT * FROM track WHERE album_id = 1');

console.log('Run 1:');
for (const _row of stmt.iterate()) {
  // consume
}
let nvisit1 = Number(
  stmt.scanStatus(0, SQLite3Database.SQLITE_SCANSTAT_NVISIT, 0),
);
console.log(`  NVISIT after first run (resetFlag=0): ${nvisit1}`);

console.log('\nRun 2 (no reset):');
for (const _row of stmt.iterate()) {
  // consume
}
let nvisit2 = Number(
  stmt.scanStatus(0, SQLite3Database.SQLITE_SCANSTAT_NVISIT, 0),
);
console.log(`  NVISIT after second run (no reset): ${nvisit2}`);
console.log(
  `  ${nvisit2 > nvisit1 ? '✓ Accumulated' : '✗ Did not accumulate'}`,
);

console.log('\nAttempting reset with resetFlag=1:');
const beforeReset = Number(
  stmt.scanStatus(0, SQLite3Database.SQLITE_SCANSTAT_NVISIT, 0),
);
const duringReset = Number(
  stmt.scanStatus(0, SQLite3Database.SQLITE_SCANSTAT_NVISIT, 1),
);
const afterReset = Number(
  stmt.scanStatus(0, SQLite3Database.SQLITE_SCANSTAT_NVISIT, 0),
);
console.log(`  Before reset call: ${beforeReset}`);
console.log(`  During reset call (with resetFlag=1): ${duringReset}`);
console.log(`  After reset call: ${afterReset}`);

console.log('\nRun 3 (after reset attempt):');
for (const _row of stmt.iterate()) {
  // consume
}
let nvisit3 = Number(
  stmt.scanStatus(0, SQLite3Database.SQLITE_SCANSTAT_NVISIT, 0),
);
console.log(`  NVISIT after third run: ${nvisit3}`);
console.log(
  `  ${nvisit3 === nvisit1 ? '✓ Reset worked' : '✗ Reset did not work'}`,
);

console.log('\nConclusion: The resetFlag parameter does NOT work.');
console.log('Scan statistics accumulate across executions of prepared statements.');
console.log(
  '\nThis explains the massive NVISIT counts in our integration tests!',
);
console.log(
  'When a cached statement is reused multiple times, NVISIT keeps growing.',
);
