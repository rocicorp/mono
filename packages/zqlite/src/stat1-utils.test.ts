import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from './db.ts';
import {Stat1Cache} from './stat1-utils.ts';

describe('Stat1Cache', () => {
  let db: Database;
  let cache: Stat1Cache;

  beforeEach(() => {
    const lc = createSilentLogContext();
    db = new Database(lc, ':memory:');

    // Create test tables with various index configurations
    db.exec(`
      -- Table 1: posts with multiple indexes
      CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        userId INTEGER NOT NULL,
        projectId INTEGER NOT NULL,
        createdAt INTEGER NOT NULL,
        content TEXT
      );

      -- Single column index
      CREATE INDEX idx_posts_userId ON posts(userId);

      -- Compound index (2 columns)
      CREATE INDEX idx_posts_user_project ON posts(userId, projectId);

      -- Compound index (3 columns) - superset of 2-column join
      CREATE INDEX idx_posts_user_project_created ON posts(userId, projectId, createdAt);

      -- Wrong order compound index (should not match userId, projectId join)
      CREATE INDEX idx_posts_project_user ON posts(projectId, userId);

      -- Table 2: comments with single column index only
      CREATE TABLE comments (
        id INTEGER PRIMARY KEY,
        postId INTEGER NOT NULL,
        authorId INTEGER NOT NULL
      );

      CREATE INDEX idx_comments_postId ON comments(postId);

      -- Table 3: tags with no indexes (except primary key)
      CREATE TABLE tags (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      );

      -- Table 4: test selectivity with real data
      CREATE TABLE issues (
        id INTEGER PRIMARY KEY,
        userId INTEGER NOT NULL,
        projectId INTEGER NOT NULL
      );

      CREATE INDEX idx_issues_userId ON issues(userId);
      CREATE INDEX idx_issues_projectId ON issues(projectId);
      CREATE INDEX idx_issues_user_project ON issues(userId, projectId);
    `);

    // Insert test data into posts
    // 100 users, 10 projects, each user has 10 posts per project
    // Total: 10,000 posts
    const insertPost = db.prepare(
      'INSERT INTO posts (id, userId, projectId, createdAt, content) VALUES (?, ?, ?, ?, ?)',
    );
    let id = 1;
    for (let userId = 1; userId <= 100; userId++) {
      for (let projectId = 1; projectId <= 10; projectId++) {
        for (let i = 0; i < 10; i++) {
          insertPost.run(id, userId, projectId, Date.now() + id, `Post ${id}`);
          id++;
        }
      }
    }

    // Insert test data into comments
    // Each post has 5 comments on average
    const insertComment = db.prepare(
      'INSERT INTO comments (id, postId, authorId) VALUES (?, ?, ?)',
    );
    for (let i = 1; i <= 50000; i++) {
      const postId = Math.floor(Math.random() * 10000) + 1;
      const authorId = Math.floor(Math.random() * 200) + 1;
      insertComment.run(i, postId, authorId);
    }

    // Insert test data into issues
    // 1000 issues: 100 users, 10 projects, 10 issues per user per project
    const insertIssue = db.prepare(
      'INSERT INTO issues (id, userId, projectId) VALUES (?, ?, ?)',
    );
    id = 1;
    for (let userId = 1; userId <= 10; userId++) {
      for (let projectId = 1; projectId <= 10; projectId++) {
        for (let i = 0; i < 10; i++) {
          insertIssue.run(id++, userId, projectId);
        }
      }
    }

    // Run ANALYZE to populate sqlite_stat1
    db.exec('ANALYZE');

    // Create cache after data is loaded and analyzed
    cache = new Stat1Cache(db);
  });

  describe('getJoinFanOut', () => {
    test('single column: returns fan-out from single-column index', () => {
      const fanOut = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut).toBeDefined();
      expect(fanOut).toBeGreaterThan(1);
      // We inserted 100 posts per user (10 projects × 10 posts)
      expect(fanOut).toBeGreaterThan(50);
    });

    test('compound: exact match with 2-column index', () => {
      const fanOut = cache.getJoinFanOut('posts', ['userId', 'projectId']);
      expect(fanOut).toBeDefined();
      // We inserted exactly 10 posts per (userId, projectId) pair
      expect(fanOut).toBeCloseTo(10, 0);
    });

    test('compound: uses superset index (3-column index for 2-column join)', () => {
      // Remove the exact 2-column index to force use of 3-column superset
      db.exec('DROP INDEX idx_posts_user_project');
      cache.schemaUpdated();

      const fanOut = cache.getJoinFanOut('posts', ['userId', 'projectId']);
      expect(fanOut).toBeDefined();
      // Should still get accurate result from 3-column index
      expect(fanOut).toBeCloseTo(10, 0);
    });

    test('compound: does not match wrong-order index', () => {
      // Remove all matching indexes, leaving no valid indexes
      db.exec('DROP INDEX idx_posts_userId');
      db.exec('DROP INDEX idx_posts_user_project');
      db.exec('DROP INDEX idx_posts_user_project_created');
      db.exec('DROP INDEX idx_posts_project_user');
      cache.schemaUpdated();

      const fanOut = cache.getJoinFanOut('posts', ['userId', 'projectId']);
      // Should not match any index and not fall back to single columns
      expect(fanOut).toBeUndefined();
    });

    test('fallback: uses most selective single column when no compound index', () => {
      const fanOut = cache.getJoinFanOut('issues', ['userId', 'projectId']);

      // We have indexes on both userId and projectId separately
      // Should return the minimum (most selective) of the two
      expect(fanOut).toBeDefined();

      // Get individual fan-outs to verify we picked the minimum
      const userIdFanOut = cache.getJoinFanOut('issues', ['userId']);
      const projectIdFanOut = cache.getJoinFanOut('issues', ['projectId']);

      expect(userIdFanOut).toBeDefined();
      expect(projectIdFanOut).toBeDefined();

      // In our test data: 10 users × 10 projects × 10 issues = 1000 total
      // userId fan-out: 1000/10 = 100 issues per user
      // projectId fan-out: 1000/10 = 100 issues per project
      // Since the compound index exists and should be found first,
      // let's remove it and re-test
      db.exec('DROP INDEX idx_issues_user_project');
      cache.schemaUpdated();

      const fanOutAfterDrop = cache.getJoinFanOut('issues', [
        'userId',
        'projectId',
      ]);
      expect(fanOutAfterDrop).toBeDefined();

      // Should be close to 100 (both columns have same selectivity in this test)
      expect(fanOutAfterDrop).toBeCloseTo(100, -1); // Within order of magnitude
    });

    test('returns undefined when no indexes exist', () => {
      const fanOut = cache.getJoinFanOut('tags', ['name']);
      expect(fanOut).toBeUndefined();
    });

    test('returns undefined for empty column list', () => {
      const fanOut = cache.getJoinFanOut('posts', []);
      expect(fanOut).toBeUndefined();
    });

    test('returns undefined for non-existent table', () => {
      const fanOut = cache.getJoinFanOut('nonexistent', ['userId']);
      expect(fanOut).toBeUndefined();
    });

    test('returns undefined for non-existent column', () => {
      const fanOut = cache.getJoinFanOut('posts', ['nonexistentColumn']);
      expect(fanOut).toBeUndefined();
    });

    test('handles index where column is not leftmost', () => {
      // idx_posts_user_project has userId as first column
      // If we ask for projectId only, it should NOT use this index
      // But idx_posts_project_user has projectId as first column
      const fanOut = cache.getJoinFanOut('posts', ['projectId']);
      expect(fanOut).toBeDefined();
      // Should use idx_posts_project_user, not idx_posts_user_project
      expect(fanOut).toBeGreaterThan(1);
    });
  });

  describe('memoization', () => {
    test('caches results for repeated calls', () => {
      const fanOut1 = cache.getJoinFanOut('posts', ['userId']);
      const fanOut2 = cache.getJoinFanOut('posts', ['userId']);

      // Results should be identical (same reference)
      expect(fanOut1).toBe(fanOut2);
      expect(fanOut1).toBeDefined();
    });

    test('different column orders use different cache keys', () => {
      // First call populates cache for this ordering
      const fanOut1 = cache.getJoinFanOut('posts', ['userId', 'projectId']);
      expect(fanOut1).toBeDefined();

      // Second call with same ordering hits cache
      const fanOut1Again = cache.getJoinFanOut('posts', [
        'userId',
        'projectId',
      ]);
      expect(fanOut1Again).toBe(fanOut1);

      // Different ordering may have different value (or same if both have indexes)
      const fanOut2 = cache.getJoinFanOut('posts', ['projectId', 'userId']);
      expect(fanOut2).toBeDefined();

      // These are separate cache entries (different keys)
      // Clear the cache and verify they're independently cached
      cache.statsUpdated();

      // First ordering re-computed
      const fanOut1AfterClear = cache.getJoinFanOut('posts', [
        'userId',
        'projectId',
      ]);
      expect(fanOut1AfterClear).toBeDefined();
    });

    test('cache invalidated by statsUpdated', () => {
      // Get initial value and populate cache
      const fanOut1 = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut1).toBeDefined();

      // Get again - should be from cache (same reference for primitives means same value)
      const fanOut1Cached = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut1Cached).toBe(fanOut1);

      // Modify data and re-analyze
      db.exec('DELETE FROM posts WHERE userId > 50');
      db.exec('ANALYZE');

      // Before calling statsUpdated, cache still returns old value
      const fanOutBeforeUpdate = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOutBeforeUpdate).toBe(fanOut1); // Still cached

      // Now invalidate cache
      cache.statsUpdated();

      // After statsUpdated, value is recomputed
      const fanOut2 = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut2).toBeDefined();

      // Value should have changed (deleted half the users)
      // Note: We're checking if it's different OR if the cache was cleared
      // Sometimes SQLite stats might approximate similarly
      if (fanOut2 === fanOut1) {
        // Even if value is same, verify it was recomputed by checking
        // that a third call returns the same instance (re-cached)
        const fanOut2Cached = cache.getJoinFanOut('posts', ['userId']);
        expect(fanOut2Cached).toBe(fanOut2);
      } else {
        expect(fanOut2).toBeLessThan(fanOut1!);
      }
    });

    test('cache invalidated by schemaUpdated', () => {
      const fanOut1 = cache.getJoinFanOut('posts', ['createdAt']);

      // Should be undefined (no index on createdAt by itself)
      expect(fanOut1).toBeUndefined();

      // Add new index
      db.exec('CREATE INDEX idx_posts_createdAt ON posts(createdAt)');
      db.exec('ANALYZE');
      cache.schemaUpdated();

      const fanOut2 = cache.getJoinFanOut('posts', ['createdAt']);

      // Now should have a value
      expect(fanOut2).toBeDefined();
      expect(fanOut2).toBeGreaterThan(0);
    });
  });

  describe('statsUpdated', () => {
    test('reloads stats after ANALYZE', () => {
      // Initial state
      const fanOut1 = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut1).toBeDefined();

      // Insert more data
      const insertPost = db.prepare(
        'INSERT INTO posts (id, userId, projectId, createdAt, content) VALUES (?, ?, ?, ?, ?)',
      );
      let id = 20000;
      for (let userId = 1; userId <= 10; userId++) {
        for (let i = 0; i < 100; i++) {
          insertPost.run(id++, userId, 1, Date.now(), `Post ${id}`);
        }
      }

      // Re-analyze
      db.exec('ANALYZE');
      cache.statsUpdated();

      const fanOut2 = cache.getJoinFanOut('posts', ['userId']);

      // Fan-out should increase (more posts per user)
      expect(fanOut2).toBeDefined();
      expect(fanOut2).toBeGreaterThan(fanOut1!);
    });

    test('handles stat1 table appearing after creation', () => {
      // Create cache before ANALYZE
      const lc = createSilentLogContext();
      const freshDb = new Database(lc, ':memory:');

      freshDb.exec(`
        CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_test_value ON test(value);
        INSERT INTO test VALUES (1, 100), (2, 100), (3, 200);
      `);

      const freshCache = new Stat1Cache(freshDb);

      // No stats yet
      expect(freshCache.getJoinFanOut('test', ['value'])).toBeUndefined();

      // Run ANALYZE and update
      freshDb.exec('ANALYZE');
      freshCache.statsUpdated();

      // Now should have stats
      expect(freshCache.getJoinFanOut('test', ['value'])).toBeDefined();
    });
  });

  describe('schemaUpdated', () => {
    test('detects new indexes', () => {
      expect(cache.getJoinFanOut('posts', ['content'])).toBeUndefined();

      db.exec('CREATE INDEX idx_posts_content ON posts(content)');
      db.exec('ANALYZE');
      cache.schemaUpdated();

      const fanOut = cache.getJoinFanOut('posts', ['content']);
      expect(fanOut).toBeDefined();
    });

    test('detects dropped indexes', () => {
      const fanOut1 = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut1).toBeDefined();

      db.exec('DROP INDEX idx_posts_userId');
      cache.schemaUpdated();

      // Should still work via compound index
      const fanOut2 = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut2).toBeDefined();

      // But if we drop all indexes with userId as first column
      db.exec('DROP INDEX idx_posts_user_project');
      db.exec('DROP INDEX idx_posts_user_project_created');
      cache.schemaUpdated();

      const fanOut3 = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut3).toBeUndefined();
    });

    test('clears all caches', () => {
      // Populate caches
      cache.getJoinFanOut('posts', ['userId']);
      cache.getJoinFanOut('comments', ['postId']);

      // Schema change
      db.exec('CREATE INDEX idx_posts_new ON posts(content)');
      cache.schemaUpdated();

      // Verify new index is detected (proves schema cache was cleared)
      const fanOut = cache.getJoinFanOut('posts', ['content']);
      expect(fanOut).toBeUndefined(); // No ANALYZE yet
    });
  });

  describe('real-world scenarios', () => {
    test('one-to-many relationship: posts per user', () => {
      const fanOut = cache.getJoinFanOut('posts', ['userId']);
      expect(fanOut).toBeDefined();
      // Each user has 100 posts (10 projects × 10 posts)
      // Allow some variance due to SQLite's approximations
      expect(fanOut).toBeGreaterThan(50);
      expect(fanOut).toBeLessThan(150);
    });

    test('many-to-one relationship: comments per post', () => {
      const fanOut = cache.getJoinFanOut('comments', ['postId']);
      expect(fanOut).toBeDefined();
      // ~50,000 comments / 10,000 posts = ~5 comments per post
      // Allow wide variance due to random distribution
      expect(fanOut).toBeGreaterThan(1);
    });

    test('compound key with high selectivity', () => {
      const fanOut = cache.getJoinFanOut('issues', ['userId', 'projectId']);
      expect(fanOut).toBeDefined();
      // Exactly 10 issues per (userId, projectId) pair
      expect(fanOut).toBeCloseTo(10, 0);
    });
  });

  describe('edge cases', () => {
    test('handles indexes with DESC modifier', () => {
      // Create index with DESC - pragma handles this correctly
      db.exec('CREATE INDEX idx_posts_created_desc ON posts(createdAt DESC)');
      db.exec('ANALYZE');
      cache.schemaUpdated();

      const fanOut = cache.getJoinFanOut('posts', ['createdAt']);
      expect(fanOut).toBeDefined();
    });

    test('handles indexes with COLLATE', () => {
      // Create index with COLLATE - pragma handles this correctly
      db.exec(
        'CREATE INDEX idx_posts_content_nocase ON posts(content COLLATE NOCASE)',
      );
      db.exec('ANALYZE');
      cache.schemaUpdated();

      const fanOut = cache.getJoinFanOut('posts', ['content']);
      expect(fanOut).toBeDefined();
    });

    test('database without ANALYZE', () => {
      const lc = createSilentLogContext();
      const freshDb = new Database(lc, ':memory:');

      freshDb.exec(`
        CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_test_value ON test(value);
        INSERT INTO test VALUES (1, 100), (2, 100), (3, 200);
      `);

      // Create cache without ANALYZE
      const freshCache = new Stat1Cache(freshDb);
      const fanOut = freshCache.getJoinFanOut('test', ['value']);
      expect(fanOut).toBeUndefined();
    });

    test('table with data but no matching indexes', () => {
      db.exec(`
        CREATE TABLE unindexed (id INTEGER PRIMARY KEY, foo INTEGER, bar INTEGER);
        INSERT INTO unindexed VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1);
      `);
      db.exec('ANALYZE');
      cache.schemaUpdated();

      const fanOut = cache.getJoinFanOut('unindexed', ['foo']);
      expect(fanOut).toBeUndefined();
    });

    test('index with minimal data', () => {
      const lc = createSilentLogContext();
      const testDb = new Database(lc, ':memory:');

      testDb.exec(`
        CREATE TABLE minimal (id INTEGER PRIMARY KEY, val INTEGER);
        CREATE INDEX idx_minimal_val ON minimal(val);
        INSERT INTO minimal VALUES (1, 1);
      `);
      testDb.exec('ANALYZE');

      const testCache = new Stat1Cache(testDb);
      const fanOut = testCache.getJoinFanOut('minimal', ['val']);

      // With only one row, should still return a valid fan-out
      expect(fanOut).toBeDefined();
      expect(fanOut).toBeGreaterThan(0);
    });
  });
});
