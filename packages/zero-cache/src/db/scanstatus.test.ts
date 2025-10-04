import SQLite3Database from '@rocicorp/zero-sqlite3';
import {afterEach, beforeEach, describe, test} from 'vitest';

/**
 * Tests to explore SQLite's scanstatus functionality for query cost estimation.
 * These tests demonstrate how different indexing strategies affect query costs
 * with and without ANALYZE statistics.
 */
describe('scanstatus - query cost estimation', () => {
  let db: SQLite3Database.Database;

  beforeEach(() => {
    db = new SQLite3Database(':memory:');

    // Enable scanstatus
    db.exec('PRAGMA query_only = OFF');

    // Create a table with test data
    db.exec(`
      CREATE TABLE items (
        id INTEGER PRIMARY KEY,
        category TEXT,
        priority INTEGER,
        name TEXT,
        created_at INTEGER
      )
    `);

    // Insert test data (1000 rows)
    const stmt = db.prepare(`
      INSERT INTO items (id, category, priority, name, created_at)
      VALUES (?, ?, ?, ?, ?)
    `);

    for (let i = 1; i <= 1000; i++) {
      const category = ['electronics', 'books', 'clothing'][i % 3];
      const priority = (i % 10) + 1;
      stmt.run(i, category, priority, `item-${i}`, Date.now() - i * 1000);
    }
  });

  afterEach(() => {
    db.close();
  });

  function logScanStatus(query: string, stmt: SQLite3Database.Statement) {
    console.log('\n========================================');
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
      try {
        const est = stmt.scanStatusV2(
          i,
          SQLite3Database.SQLITE_SCANSTAT_EST,
          0,
        ) as number | undefined;

        const nLoop = stmt.scanStatusV2(
          i,
          SQLite3Database.SQLITE_SCANSTAT_NLOOP,
          0,
        ) as number | undefined;

        const nVisit = stmt.scanStatusV2(
          i,
          SQLite3Database.SQLITE_SCANSTAT_NVISIT,
          0,
        ) as number | undefined;

        const name = stmt.scanStatusV2(
          i,
          SQLite3Database.SQLITE_SCANSTAT_NAME,
          0,
        ) as string | undefined;

        const explainQueryPlan = stmt.scanStatusV2(
          i,
          SQLite3Database.SQLITE_SCANSTAT_EXPLAIN,
          0,
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
      } catch (e) {
        // No more loops or error occurred
        break;
      }
    }

    console.log('Number of loops:', loopCount);
    console.log('\nScan Statistics:');
    if (stats.length === 0) {
      console.log('  No scanstatus data available (may need to compile SQLite with SQLITE_ENABLE_STMT_SCANSTATUS)');
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

  function runQueryWithScanStatus(query: string) {
    const stmt = db.prepare(query);

    // Reset statement first
    stmt.all();

    // According to SQLite docs, scanstatus is populated during query execution
    // We need to execute the query and scanstatus will be available
    logScanStatus(query, stmt);
  }

  describe('without ANALYZE', () => {
    test('query with LIMIT', () => {
      runQueryWithScanStatus('SELECT * FROM items LIMIT 10');
    });

    test('query without LIMIT', () => {
      runQueryWithScanStatus('SELECT * FROM items');
    });

    test('ordered by indexed field (PRIMARY KEY) with LIMIT', () => {
      runQueryWithScanStatus('SELECT * FROM items ORDER BY id LIMIT 10');
    });

    test('ordered by indexed field (PRIMARY KEY) without LIMIT', () => {
      runQueryWithScanStatus('SELECT * FROM items ORDER BY id');
    });

    test('ordered by non-indexed field with LIMIT', () => {
      runQueryWithScanStatus('SELECT * FROM items ORDER BY created_at LIMIT 10');
    });

    test('ordered by non-indexed field without LIMIT', () => {
      runQueryWithScanStatus('SELECT * FROM items ORDER BY created_at');
    });
  });

  describe('with single-column indexes', () => {
    beforeEach(() => {
      db.exec('CREATE INDEX idx_category ON items(category)');
      db.exec('CREATE INDEX idx_priority ON items(priority)');
      db.exec('CREATE INDEX idx_created_at ON items(created_at)');
    });

    describe('without ANALYZE', () => {
      test('WHERE on indexed field, ORDER BY different indexed field, with LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority LIMIT 10",
        );
      });

      test('WHERE on indexed field, ORDER BY different indexed field, without LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority",
        );
      });

      test('WHERE on indexed field, ORDER BY same field, with LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY category LIMIT 10",
        );
      });

      test('WHERE on indexed field, ORDER BY same field, without LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY category",
        );
      });
    });

    describe('with ANALYZE', () => {
      beforeEach(() => {
        db.exec('ANALYZE');
      });

      test('WHERE on indexed field, ORDER BY different indexed field, with LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority LIMIT 10",
        );
      });

      test('WHERE on indexed field, ORDER BY different indexed field, without LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority",
        );
      });

      test('WHERE on indexed field, ORDER BY same field, with LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY category LIMIT 10",
        );
      });

      test('WHERE on indexed field, ORDER BY same field, without LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY category",
        );
      });
    });
  });

  describe('with compound indexes', () => {
    beforeEach(() => {
      // Compound index on WHERE and ORDER BY columns
      db.exec('CREATE INDEX idx_compound_cat_priority ON items(category, priority)');
      db.exec('CREATE INDEX idx_compound_priority_cat ON items(priority, category)');
    });

    describe('without ANALYZE', () => {
      test('WHERE + ORDER BY on compound index (category, priority), with LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority LIMIT 10",
        );
      });

      test('WHERE + ORDER BY on compound index (category, priority), without LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority",
        );
      });

      test('WHERE + ORDER BY on reverse compound index (priority, category), with LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE priority = 5 ORDER BY category LIMIT 10",
        );
      });

      test('WHERE + ORDER BY on reverse compound index (priority, category), without LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE priority = 5 ORDER BY category",
        );
      });
    });

    describe('with ANALYZE', () => {
      beforeEach(() => {
        db.exec('ANALYZE');
      });

      test('WHERE + ORDER BY on compound index (category, priority), with LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority LIMIT 10",
        );
      });

      test('WHERE + ORDER BY on compound index (category, priority), without LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority",
        );
      });

      test('WHERE + ORDER BY on reverse compound index (priority, category), with LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE priority = 5 ORDER BY category LIMIT 10",
        );
      });

      test('WHERE + ORDER BY on reverse compound index (priority, category), without LIMIT', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE priority = 5 ORDER BY category",
        );
      });

      test('Complex query: multiple WHERE conditions with ORDER BY', () => {
        runQueryWithScanStatus(
          "SELECT * FROM items WHERE category = 'electronics' AND priority > 5 ORDER BY priority LIMIT 10",
        );
      });

      test('Query using covering index', () => {
        runQueryWithScanStatus(
          "SELECT category, priority FROM items WHERE category = 'electronics' ORDER BY priority LIMIT 10",
        );
      });
    });
  });

  describe('comparison: impact of ANALYZE on estimates', () => {
    beforeEach(() => {
      db.exec('CREATE INDEX idx_category ON items(category)');
      db.exec('CREATE INDEX idx_priority ON items(priority)');
      db.exec('CREATE INDEX idx_compound ON items(category, priority)');
    });

    test('Before ANALYZE: full table scan', () => {
      console.log('\n╔════════════════════════════════════════╗');
      console.log('║  BEFORE ANALYZE - Full Table Scan     ║');
      console.log('╚════════════════════════════════════════╝');
      runQueryWithScanStatus('SELECT * FROM items WHERE category = \'electronics\'');
    });

    test('After ANALYZE: full table scan', () => {
      db.exec('ANALYZE');
      console.log('\n╔════════════════════════════════════════╗');
      console.log('║  AFTER ANALYZE - Full Table Scan      ║');
      console.log('╚════════════════════════════════════════╝');
      runQueryWithScanStatus('SELECT * FROM items WHERE category = \'electronics\'');
    });

    test('Before ANALYZE: compound index query', () => {
      console.log('\n╔════════════════════════════════════════╗');
      console.log('║  BEFORE ANALYZE - Compound Index      ║');
      console.log('╚════════════════════════════════════════╝');
      runQueryWithScanStatus(
        "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority LIMIT 10",
      );
    });

    test('After ANALYZE: compound index query', () => {
      db.exec('ANALYZE');
      console.log('\n╔════════════════════════════════════════╗');
      console.log('║  AFTER ANALYZE - Compound Index       ║');
      console.log('╚════════════════════════════════════════╝');
      runQueryWithScanStatus(
        "SELECT * FROM items WHERE category = 'electronics' ORDER BY priority LIMIT 10",
      );
    });
  });
});
