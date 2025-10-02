// oxlint-disable no-console
import SQLite3Database from '@rocicorp/zero-sqlite3';
import chalk from 'chalk';
import {afterAll, beforeAll, describe, test} from 'vitest';

function createData(skew: boolean = false) {
  const db = new SQLite3Database(':memory:');

  // Enable scanstatus
  db.exec('PRAGMA query_only = OFF');

  // Create a table with test data
  db.exec(`
      CREATE TABLE issue (
        id INTEGER PRIMARY KEY,
        title TEXT,
        owner_id INTEGER,
        creator_id INTEGER,
        created INTEGER,
        modified INTEGER
      );
      CREATE INDEX idx_owner_modified ON issue(owner_id, modified);
      CREATE INDEX idx_creator_modified ON issue(creator_id, modified);
      CREATE INDEX idx_created ON issue(created);
      CREATE INDEX idx_modified ON issue(modified);

      CREATE TABLE user (
        id INTEGER PRIMARY KEY,
        name TEXT,
        email TEXT
      );
      CREATE UNIQUE INDEX idx_user_email ON user(email);
      CREATE INDEX idx_user_name ON user(name);
    `);

  // Insert 100 users
  const userInsert = db.prepare('INSERT INTO user (name, email) VALUES (?, ?)');
  for (let i = 1; i <= 100; i++) {
    userInsert.run(`User ${i}`, `user${i}@example.com`);
  }

  // Now insert 10_000 issues with even distribution of owner_id and creator_id
  const issueInsert = db.prepare(
    'INSERT INTO issue (title, owner_id, creator_id, created, modified) VALUES (?, ?, ?, ?, ?)',
  );
  for (let i = 1; i <= 10000; i++) {
    const ownerId = (i % 100) + 1; // owner_id between 1 and 100
    const creatorId = skew && i % 2 === 0 ? 1 : ((i + 50) % 100) + 1; // creator_id between 1 and 100, offset by 50
    const timestamp = Date.now() - i * 1000; // Decreasing timestamps
    issueInsert.run(
      `Issue ${i}`,
      ownerId,
      creatorId,
      timestamp,
      timestamp + 5000,
    );
  }

  db.exec('ANALYZE');

  return db;
}

const cases = [
  ['order by: indexed', 'SELECT * FROM issue ORDER BY created LIMIT 100', []],
  ['order by: not indexed', 'SELECT * FROM issue ORDER BY title LIMIT 100', []],
  [
    'constrain on indexed column',
    'SELECT * FROM issue WHERE owner_id = 42',
    [],
  ],
  [
    'constrain on indexed column (wildcard)',
    'SELECT * FROM issue WHERE owner_id = ?',
    [42],
  ],
  [
    'constrain and sort: not compound index, wildcard',
    'SELECT * FROM issue WHERE owner_id = ? ORDER BY created LIMIT 50',
    [42],
  ],
  [
    'constrain and sort: compound index, wildcard',
    'SELECT * FROM issue WHERE owner_id = ? ORDER BY modified LIMIT 50',
    [42],
  ],
  [
    'user table with name constraint',
    'SELECT * FROM user WHERE name = ?',
    ['User 42'],
  ],
  [
    'or yields many loops',
    'SELECT * FROM issue WHERE owner_id = 1 OR owner_id = 2 OR creator_id = 3',
    [],
  ],
  [
    'wild card or yields many loops?',
    'SELECT * FROM issue WHERE owner_id = ? OR owner_id = ? OR creator_id = ?',
    undefined,
  ],
  [
    'or with sort',
    'SELECT * FROM issue WHERE owner_id = ? OR owner_id = ? OR creator_id = ? ORDER BY modified',
    undefined,
  ],
  ['user table, no constraint', 'SELECT * FROM user ORDER BY name', []],
] as const;

function plans(db: SQLite3Database.Database) {
  const plan1 = `issue.whereExists('creator', q => q.whereName('User 1')).orderBy('modified').limit(50)`;
  const plan2 = `issue.whereExists('creator', q => q.whereName('User 1')).orderBy('created').limit(50)`;

  console.log(chalk.bold.red(`Plan: ${plan1}`));
  runQueryWithScanStatus(db, '', 'SELECT * FROM issue ORDER BY modified', []);
  runQueryWithScanStatus(db, '', 'SELECT * FROM user WHERE name = ?', [
    'User 1',
  ]);
  runQueryWithScanStatus(
    db,
    '',
    'SELECT * FROM issue WHERE creator_id = ? ORDER BY modified',
    undefined,
  );

  console.log(chalk.bold.red(`Plan: ${plan2}`));
  runQueryWithScanStatus(db, '', 'SELECT * FROM issue ORDER BY created', []);
  runQueryWithScanStatus(db, '', 'SELECT * FROM user WHERE name = ?', [
    'User 1',
  ]);
  runQueryWithScanStatus(
    db,
    '',
    'SELECT * FROM issue WHERE creator_id = ? ORDER BY created',
    undefined, // we do now know the binding value ahead of time right now
  );

  console.log(
    'PLAN: ',
    db
      .prepare(
        'EXPLAIN QUERY PLAN SELECT * FROM issue WHERE owner_id = ? OR owner_id = ? OR creator_id = ? ORDER BY modified',
      )
      .all([1, 2, 3]),
  );
}

/**
 * Tests to explore SQLite's scanstatus functionality for query cost estimation.
 * These tests demonstrate how different indexing strategies affect query costs
 * with and without ANALYZE statistics.
 */
describe('scanstatus - query cost estimation', () => {
  let db: SQLite3Database.Database;
  beforeAll(() => {
    db = createData();
  });
  afterAll(() => {
    db.close();
  });

  test.each(cases)('%1', (name, query, args) => {
    runQueryWithScanStatus(db, name, query, args);
  });

  test('plans', () => plans(db));
});

describe('skewed', () => {
  let db: SQLite3Database.Database;
  beforeAll(() => {
    db = createData(true);
  });
  afterAll(() => {
    db.close();
  });

  console.log(chalk.bold.blue('\n=== Skewed Data ===\n'));
  test('plans', () => plans(db));
});

function logScanStatus(
  name: string,
  query: string,
  stmt: SQLite3Database.Statement,
) {
  console.log('\n========================================');
  if (name) {
    console.log('Test:', name);
  }
  console.log('Query:', query);
  console.log('========================================');

  let loopCount = 0;
  const stats: Array<{
    loop: number;
    est: number;
    nLoop: number;
    nVisit: number;
    name: string;
    explainQueryPlan: string;
  }> = [];

  // Try to collect scanstatus for each loop
  for (let i = 0; i < 100; i++) {
    const est = stmt.scanStatusV2(i, SQLite3Database.SQLITE_SCANSTAT_EST, 1) as
      | number
      | undefined;

    const nLoop = stmt.scanStatusV2(
      i,
      SQLite3Database.SQLITE_SCANSTAT_NLOOP,
      1,
    ) as number | undefined;

    const nVisit = stmt.scanStatusV2(
      i,
      SQLite3Database.SQLITE_SCANSTAT_NVISIT,
      1,
    ) as number | undefined;

    const name = stmt.scanStatusV2(
      i,
      SQLite3Database.SQLITE_SCANSTAT_NAME,
      1,
    ) as string | undefined;

    const explainQueryPlan = stmt.scanStatusV2(
      i,
      SQLite3Database.SQLITE_SCANSTAT_EXPLAIN,
      1,
    ) as string | undefined;

    // Skip if we got undefined (no more loops)
    if (est === undefined) {
      break;
    }

    stats.push({
      loop: i,
      est: est ?? 0,
      nLoop: nLoop ?? 0,
      nVisit: nVisit ?? 0,
      name: name ?? 'unknown',
      explainQueryPlan: explainQueryPlan ?? 'N/A',
    });

    loopCount++;
  }

  console.log('Number of loops:', loopCount);
  console.log('\nScan Statistics:');
  if (stats.length === 0) {
    console.log(
      '  No scanstatus data available (may need to compile SQLite with SQLITE_ENABLE_STMT_SCANSTATUS)',
    );
  }
  stats.forEach(stat => {
    console.log(`  Loop ${stat.loop}:`);
    console.log(`    Estimated rows: ${stat.est}`);
    console.log(`    Actual loops (nLoop): ${stat.nLoop}`);
    console.log(`    Rows visited (nVisit): ${stat.nVisit}`);
    console.log(`    Table/Index: ${stat.name}`);
    console.log(`    Explain: ${stat.explainQueryPlan}`);
  });
  console.log('========================================\n');
}

function runQueryWithScanStatus(
  db: SQLite3Database.Database,
  name: string,
  query: string,
  // if args is undefined we do not run the query and just gather estimates instead.
  args: readonly unknown[] | undefined,
) {
  const stmt = db.prepare(query);

  // Reset statement first
  if (args !== undefined) {
    stmt.all(...args);
  }

  // According to SQLite docs, scanstatus is populated during query execution
  // We need to execute the query and scanstatus will be available
  logScanStatus(name, query, stmt);
}
