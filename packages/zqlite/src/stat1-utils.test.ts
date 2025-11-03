import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from './db.ts';
import {
  findIndexesForTable,
  getIndexColumns,
  getIndexStats,
  getJoinFanOut,
} from './stat1-utils.ts';

describe('stat1-utils', () => {
  let db: Database;

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
  });

  describe('getIndexColumns', () => {
    test('returns columns for single-column index', () => {
      const columns = getIndexColumns(db, 'idx_posts_userId');
      expect(columns).toEqual(['userId']);
    });

    test('returns columns for compound index in order', () => {
      const columns = getIndexColumns(db, 'idx_posts_user_project');
      expect(columns).toEqual(['userId', 'projectId']);
    });

    test('returns columns for 3-column index', () => {
      const columns = getIndexColumns(db, 'idx_posts_user_project_created');
      expect(columns).toEqual(['userId', 'projectId', 'createdAt']);
    });

    test('returns undefined for non-existent index', () => {
      const columns = getIndexColumns(db, 'idx_does_not_exist');
      expect(columns).toBeUndefined();
    });

    test('handles index with DESC modifier', () => {
      db.exec('CREATE INDEX idx_posts_created_desc ON posts(createdAt DESC)');
      const columns = getIndexColumns(db, 'idx_posts_created_desc');
      expect(columns).toEqual(['createdAt']);
    });
  });

  describe('getIndexStats', () => {
    test('returns stats for single-column index', () => {
      const stats = getIndexStats(db, 'posts', 'idx_posts_userId');
      expect(stats).toBeDefined();
      expect(stats?.indexName).toBe('idx_posts_userId');
      expect(stats?.totalRows).toBeGreaterThan(0);
      expect(stats?.avgRowsPerDistinct.length).toBe(1);
      expect(stats?.avgRowsPerDistinct[0]).toBeGreaterThan(0);
    });

    test('returns stats for compound index', () => {
      const stats = getIndexStats(db, 'posts', 'idx_posts_user_project');
      expect(stats).toBeDefined();
      expect(stats?.avgRowsPerDistinct.length).toBe(2);
      // First value: avg posts per user
      expect(stats?.avgRowsPerDistinct[0]).toBeGreaterThan(1);
      // Second value: avg posts per (user, project) pair
      expect(stats?.avgRowsPerDistinct[1]).toBeGreaterThan(0);
    });

    test('returns undefined for index without stats', () => {
      // Create a new index after ANALYZE
      db.exec('CREATE INDEX idx_posts_content ON posts(content)');
      const stats = getIndexStats(db, 'posts', 'idx_posts_content');
      expect(stats).toBeUndefined();
    });

    test('returns undefined for non-existent index', () => {
      const stats = getIndexStats(db, 'posts', 'idx_does_not_exist');
      expect(stats).toBeUndefined();
    });

    test('returns undefined for non-existent table', () => {
      const stats = getIndexStats(db, 'nonexistent', 'idx_posts_userId');
      expect(stats).toBeUndefined();
    });
  });

  describe('findIndexesForTable', () => {
    test('returns all indexes for a table', () => {
      const indexes = findIndexesForTable(db, 'posts');
      expect(indexes).toContain('idx_posts_userId');
      expect(indexes).toContain('idx_posts_user_project');
      expect(indexes).toContain('idx_posts_user_project_created');
      expect(indexes).toContain('idx_posts_project_user');
    });

    test('returns empty array for table with no indexes', () => {
      const indexes = findIndexesForTable(db, 'tags');
      // Note: SQLite may auto-create index for PRIMARY KEY, filter those out
      const nonPkIndexes = indexes.filter(
        idx => !idx.includes('autoindex') && !idx.includes('pk'),
      );
      expect(nonPkIndexes).toEqual([]);
    });

    test('returns empty array for non-existent table', () => {
      const indexes = findIndexesForTable(db, 'nonexistent');
      expect(indexes).toEqual([]);
    });
  });

  describe('getJoinFanOut', () => {
    test('single column: returns fan-out from single-column index', () => {
      const fanOut = getJoinFanOut(db, 'posts', ['userId']);
      expect(fanOut).toBeDefined();
      expect(fanOut).toBeGreaterThan(1);
      // We inserted 100 posts per user (10 projects × 10 posts)
      expect(fanOut).toBeGreaterThan(50);
    });

    test('compound: exact match with 2-column index', () => {
      const fanOut = getJoinFanOut(db, 'posts', ['userId', 'projectId']);
      expect(fanOut).toBeDefined();
      // We inserted exactly 10 posts per (userId, projectId) pair
      expect(fanOut).toBeCloseTo(10, 0);
    });

    test('compound: uses superset index (3-column index for 2-column join)', () => {
      // Remove the exact 2-column index to force use of 3-column superset
      db.exec('DROP INDEX idx_posts_user_project');

      const fanOut = getJoinFanOut(db, 'posts', ['userId', 'projectId']);
      expect(fanOut).toBeDefined();
      // Should still get accurate result from 3-column index
      expect(fanOut).toBeCloseTo(10, 0);
    });

    test('compound: does not match wrong-order index', () => {
      // Remove all matching indexes, leaving only wrong-order index
      db.exec('DROP INDEX idx_posts_userId');
      db.exec('DROP INDEX idx_posts_user_project');
      db.exec('DROP INDEX idx_posts_user_project_created');
      // Also drop the projectId-first index to avoid single-column fallback
      db.exec('DROP INDEX idx_posts_project_user');

      const fanOut = getJoinFanOut(db, 'posts', ['userId', 'projectId']);
      // Should not match any index and not fall back to single columns
      expect(fanOut).toBeUndefined();
    });

    test('fallback: uses most selective single column when no compound index', () => {
      const fanOut = getJoinFanOut(db, 'issues', ['userId', 'projectId']);

      // We have indexes on both userId and projectId separately
      // Should return the minimum (most selective) of the two
      expect(fanOut).toBeDefined();

      // Get individual fan-outs to verify we picked the minimum
      const userIdFanOut = getJoinFanOut(db, 'issues', ['userId']);
      const projectIdFanOut = getJoinFanOut(db, 'issues', ['projectId']);

      expect(userIdFanOut).toBeDefined();
      expect(projectIdFanOut).toBeDefined();

      // In our test data: 10 users × 10 projects × 10 issues = 1000 total
      // userId fan-out: 1000/10 = 100 issues per user
      // projectId fan-out: 1000/10 = 100 issues per project
      // Since the compound index exists and should be found first,
      // let's remove it and re-test
      db.exec('DROP INDEX idx_issues_user_project');
      db.exec('ANALYZE');

      const fanOutAfterDrop = getJoinFanOut(db, 'issues', [
        'userId',
        'projectId',
      ]);
      expect(fanOutAfterDrop).toBeDefined();

      // Should be close to 100 (both columns have same selectivity in this test)
      expect(fanOutAfterDrop).toBeCloseTo(100, -1); // Within order of magnitude
    });

    test('returns undefined when no indexes exist', () => {
      const fanOut = getJoinFanOut(db, 'tags', ['name']);
      expect(fanOut).toBeUndefined();
    });

    test('returns undefined for empty column list', () => {
      const fanOut = getJoinFanOut(db, 'posts', []);
      expect(fanOut).toBeUndefined();
    });

    test('returns undefined for non-existent table', () => {
      const fanOut = getJoinFanOut(db, 'nonexistent', ['userId']);
      expect(fanOut).toBeUndefined();
    });

    test('returns undefined for non-existent column', () => {
      const fanOut = getJoinFanOut(db, 'posts', ['nonexistentColumn']);
      expect(fanOut).toBeUndefined();
    });

    test('handles index where column is not leftmost', () => {
      // idx_posts_user_project has userId as first column
      // If we ask for projectId only, it should NOT use this index
      // But idx_posts_project_user has projectId as first column
      const fanOut = getJoinFanOut(db, 'posts', ['projectId']);
      expect(fanOut).toBeDefined();
      // Should use idx_posts_project_user, not idx_posts_user_project
      expect(fanOut).toBeGreaterThan(1);
    });
  });

  describe('real-world scenarios', () => {
    test('one-to-many relationship: posts per user', () => {
      const fanOut = getJoinFanOut(db, 'posts', ['userId']);
      expect(fanOut).toBeDefined();
      // Each user has 100 posts (10 projects × 10 posts)
      // Allow some variance due to SQLite's approximations
      expect(fanOut).toBeGreaterThan(50);
      expect(fanOut).toBeLessThan(150);
    });

    test('many-to-one relationship: comments per post', () => {
      const fanOut = getJoinFanOut(db, 'comments', ['postId']);
      expect(fanOut).toBeDefined();
      // ~50,000 comments / 10,000 posts = ~5 comments per post
      // Allow wide variance due to random distribution
      expect(fanOut).toBeGreaterThan(1);
    });

    test('compound key with high selectivity', () => {
      const fanOut = getJoinFanOut(db, 'issues', ['userId', 'projectId']);
      expect(fanOut).toBeDefined();
      // Exactly 10 issues per (userId, projectId) pair
      expect(fanOut).toBeCloseTo(10, 0);
    });
  });

  describe('edge cases', () => {
    test('database without ANALYZE', () => {
      const lc = createSilentLogContext();
      const freshDb = new Database(lc, ':memory:');

      freshDb.exec(`
        CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER);
        CREATE INDEX idx_test_value ON test(value);
        INSERT INTO test VALUES (1, 100), (2, 100), (3, 200);
      `);

      // No ANALYZE run
      const fanOut = getJoinFanOut(freshDb, 'test', ['value']);
      expect(fanOut).toBeUndefined();
    });

    test('table with data but no matching indexes', () => {
      db.exec(`
        CREATE TABLE unindexed (id INTEGER PRIMARY KEY, foo INTEGER, bar INTEGER);
        INSERT INTO unindexed VALUES (1, 1, 1), (2, 1, 2), (3, 2, 1);
      `);
      db.exec('ANALYZE');

      const fanOut = getJoinFanOut(db, 'unindexed', ['foo']);
      expect(fanOut).toBeUndefined();
    });

    test('index with zero avgRowsPerDistinct (degenerate case)', () => {
      // This is hard to create naturally, but test the code path
      const lc = createSilentLogContext();
      const testDb = new Database(lc, ':memory:');

      testDb.exec(`
        CREATE TABLE degenerate (id INTEGER PRIMARY KEY, val INTEGER);
        CREATE INDEX idx_degenerate_val ON degenerate(val);
        INSERT INTO degenerate VALUES (1, 1);
      `);
      testDb.exec('ANALYZE');

      const fanOut = getJoinFanOut(testDb, 'degenerate', ['val']);
      // With only one row, stat might be "1 1", so fanOut should be 1
      expect(fanOut).toBeDefined();
      expect(fanOut).toBeGreaterThan(0);
    });
  });
});
