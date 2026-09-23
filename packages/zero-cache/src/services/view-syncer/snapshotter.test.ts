import type {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs, listTables} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import {DbFile, expectTables} from '../../test/lite.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {SnapshotRowCache} from './snapshot-row-cache.ts';
import {
  InvalidDiffError,
  ResetPipelinesSignal,
  Snapshotter,
  type Change,
} from './snapshotter.ts';

describe('view-syncer/snapshotter', () => {
  let lc: LogContext;
  let dbFile: DbFile;
  let replicator: FakeReplicator;
  let tableSpecs: Map<string, LiteAndZqlSpec>;
  let allTableNames: Set<string>;
  let s: Snapshotter;

  beforeEach(() => {
    lc = createSilentLogContext();
    dbFile = new DbFile('snapshotter_test');
    const db = dbFile.connect(lc);
    db.pragma('journal_mode = WAL2');
    db.exec(/*sql*/ `
        CREATE TABLE "my_app.permissions" (
          "lock"        INT PRIMARY KEY,
          "permissions" JSON,
          "hash"        TEXT,
          _0_version    TEXT NOT NULL
        );
        INSERT INTO "my_app.permissions" ("lock", "_0_version") VALUES (1, '01');
        CREATE TABLE issues(
          id INT PRIMARY KEY,
          owner INTEGER,
          desc TEXT,
          ignore UNSUPPORTED_TYPE,
          stillBeingBackfilled TEXT,
          _0_version TEXT NOT NULL
        );
        CREATE TABLE users(id INT PRIMARY KEY, handle TEXT UNIQUE, ignore UNSUPPORTED_TYPE, _0_version TEXT NOT NULL);
        CREATE TABLE comments(id INT PRIMARY KEY, desc TEXT, ignore UNSUPPORTED_TYPE, _0_version TEXT NOT NULL);

        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(1, 10, 'foo', 'zzz', '01');
        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(2, 10, 'bar', 'xyz', '01');
        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(3, 20, 'baz', 'yyy', '01');

        INSERT INTO users(id, handle, ignore, _0_version) VALUES(10, 'alice', 'vvv', '01');
        INSERT INTO users(id, handle, ignore, _0_version) VALUES(20, 'bob', 'vxv', '01');

        CREATE TABLE backfilling(id INT PRIMARY KEY, _0_version TEXT NOT NULL);
      `);
    initReplicationState(db, ['zero_data'], '01');

    // Initialize ColumnMetadata and mark a column as being backfilled,
    // to verify that it does not appear in the pipeline results.
    populateFromExistingTables(db, listTables(db, false));
    db.prepare(/*sql*/ `
      UPDATE "_zero.column_metadata" 
        SET backfill = '{"upstreamID":123}'
        WHERE table_name = 'issues' 
         AND column_name = 'stillBeingBackfilled'
      `).run();

    tableSpecs = computeZqlSpecs(lc, db, {includeBackfillingColumns: false});
    allTableNames = new Set(tableSpecs.keys());

    replicator = fakeReplicator(lc, db);
    s = new Snapshotter(lc, dbFile.path, {appID: 'my_app'}).init();
  });

  afterEach(() => {
    s.destroy();
    dbFile.delete();
  });

  test('initial snapshot', () => {
    const {db, version} = s.current();

    expect(version).toBe('01');
    expectTables(db.db, {
      issues: [
        {
          id: 1,
          owner: 10,
          desc: 'foo',
          ignore: 'zzz',
          stillBeingBackfilled: null,
          ['_0_version']: '01',
        },
        {
          id: 2,
          owner: 10,
          desc: 'bar',
          ignore: 'xyz',
          stillBeingBackfilled: null,
          ['_0_version']: '01',
        },
        {
          id: 3,
          owner: 20,
          desc: 'baz',
          ignore: 'yyy',
          stillBeingBackfilled: null,
          ['_0_version']: '01',
        },
      ],
      users: [
        {id: 10, handle: 'alice', ignore: 'vvv', ['_0_version']: '01'},
        {id: 20, handle: 'bob', ignore: 'vxv', ['_0_version']: '01'},
      ],
    });
  });

  test('empty diff', () => {
    const {version} = s.current();

    expect(version).toBe('01');

    const diff = s.advance(tableSpecs, allTableNames);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(0);

    expect([...diff]).toEqual([]);
  });

  const messages = new ReplicationMessages({
    'issues': 'id',
    'users': 'id',
    'comments': 'id',
    'backfilling': 'id',
    ['my_app.permissions']: 'lock',
  });

  test('multiple prev values', () => {
    expect(s.current().version).toBe('01');

    replicator.processTransaction(
      '09',
      messages.insert('users', {id: 20, handle: 'alice'}),
    );
    replicator.processTransaction('09');

    const diff = s.advance(tableSpecs, allTableNames);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('09');
    expect(diff.changes).toBe(1);

    expect([...diff]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "09",
            "handle": "alice",
            "id": 20,
          },
          "prevValues": [
            {
              "_0_version": "01",
              "handle": "bob",
              "id": 20,
            },
            {
              "_0_version": "01",
              "handle": "alice",
              "id": 10,
            },
          ],
          "rowKey": {
            "id": 20,
          },
          "table": "users",
        },
      ]
    `);
  });

  test('non-syncable tables skipped', () => {
    expect(s.current().version).toBe('01');

    replicator.processTransaction(
      '09',
      messages.insert('users', {id: 20, handle: 'alice'}),
      messages.insert('backfilling', {id: 30}),
      messages.insert('users', {id: 30, handle: 'bob'}),
    );
    replicator.processTransaction('09');

    // simulate the backfilling table being non-syncable
    tableSpecs.delete('backfilling');
    const diff = s.advance(tableSpecs, allTableNames);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('09');
    expect(diff.changes).toBe(3);
    expect(diff.changesByTable()).toEqual(
      new Map([
        ['backfilling', 1],
        ['users', 2],
      ]),
    );

    expect([...diff]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "09",
            "handle": "alice",
            "id": 20,
          },
          "prevValues": [
            {
              "_0_version": "01",
              "handle": "bob",
              "id": 20,
            },
            {
              "_0_version": "01",
              "handle": "alice",
              "id": 10,
            },
          ],
          "rowKey": {
            "id": 20,
          },
          "table": "users",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "handle": "bob",
            "id": 30,
          },
          "prevValues": [
            {
              "_0_version": "01",
              "handle": "bob",
              "id": 20,
            },
          ],
          "rowKey": {
            "id": 30,
          },
          "table": "users",
        },
      ]
    `);
  });

  test('concurrent snapshot diffs', () => {
    const s1 = new Snapshotter(lc, dbFile.path, {appID: 'my_app'}).init();
    const s2 = new Snapshotter(lc, dbFile.path, {appID: 'my_app'}).init();

    expect(s1.current().version).toBe('01');
    expect(s2.current().version).toBe('01');

    replicator.processTransaction(
      '09',
      messages.insert('issues', {id: 4, owner: 20}),
      messages.update('issues', {id: 1, owner: 10, desc: 'food'}),
      messages.update('issues', {id: 5, owner: 10, desc: 'bard'}, {id: 2}),
      messages.delete('issues', {id: 3}),
    );

    const diff1 = s1.advance(tableSpecs, allTableNames);
    expect(diff1.prev.version).toBe('01');
    expect(diff1.curr.version).toBe('09');
    expect(diff1.changes).toBe(5); // The key update results in a del(old) + set(new).

    expect([...diff1]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "09",
            "desc": null,
            "id": 4,
            "owner": 20,
          },
          "prevValues": [],
          "rowKey": {
            "id": 4,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "food",
            "id": 1,
            "owner": 10,
          },
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "foo",
              "id": 1,
              "owner": 10,
            },
          ],
          "rowKey": {
            "id": 1,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "bar",
              "id": 2,
              "owner": 10,
            },
          ],
          "rowKey": {
            "id": 2,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "bard",
            "id": 5,
            "owner": 10,
          },
          "prevValues": [],
          "rowKey": {
            "id": 5,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "baz",
              "id": 3,
              "owner": 20,
            },
          ],
          "rowKey": {
            "id": 3,
          },
          "table": "issues",
        },
      ]
    `);

    // Diff should be reusable as long as advance() hasn't been called.
    expect([...diff1]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "09",
            "desc": null,
            "id": 4,
            "owner": 20,
          },
          "prevValues": [],
          "rowKey": {
            "id": 4,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "food",
            "id": 1,
            "owner": 10,
          },
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "foo",
              "id": 1,
              "owner": 10,
            },
          ],
          "rowKey": {
            "id": 1,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "bar",
              "id": 2,
              "owner": 10,
            },
          ],
          "rowKey": {
            "id": 2,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "bard",
            "id": 5,
            "owner": 10,
          },
          "prevValues": [],
          "rowKey": {
            "id": 5,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "baz",
              "id": 3,
              "owner": 20,
            },
          ],
          "rowKey": {
            "id": 3,
          },
          "table": "issues",
        },
      ]
    `);

    // Replicate a second transaction
    replicator.processTransaction(
      '0d',
      messages.delete('issues', {id: 4}),
      messages.update('issues', {id: 2, owner: 10, desc: 'bard'}, {id: 5}),
    );

    const diff2 = s1.advance(tableSpecs, allTableNames);
    expect(diff2.prev.version).toBe('09');
    expect(diff2.curr.version).toBe('0d');
    expect(diff2.changes).toBe(3);

    expect([...diff2]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": null,
          "prevValues": [
            {
              "_0_version": "09",
              "desc": null,
              "id": 4,
              "owner": 20,
            },
          ],
          "rowKey": {
            "id": 4,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValues": [
            {
              "_0_version": "09",
              "desc": "bard",
              "id": 5,
              "owner": 10,
            },
          ],
          "rowKey": {
            "id": 5,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "0d",
            "desc": "bard",
            "id": 2,
            "owner": 10,
          },
          "prevValues": [],
          "rowKey": {
            "id": 2,
          },
          "table": "issues",
        },
      ]
    `);

    // Attempting to iterate diff1 should result in an error since s1 has advanced.
    let thrown;
    try {
      [...diff1];
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(InvalidDiffError);

    // The diff for s2 goes straight from '00' to '08'.
    // This will coalesce multiple changes to a row, and can result in some noops,
    // (e.g. rows that return to their original state).
    const diff3 = s2.advance(tableSpecs, allTableNames);
    expect(diff3.prev.version).toBe('01');
    expect(diff3.curr.version).toBe('0d');
    expect(diff3.changes).toBe(5);
    expect([...diff3]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "food",
            "id": 1,
            "owner": 10,
          },
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "foo",
              "id": 1,
              "owner": 10,
            },
          ],
          "rowKey": {
            "id": 1,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "baz",
              "id": 3,
              "owner": 20,
            },
          ],
          "rowKey": {
            "id": 3,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "0d",
            "desc": "bard",
            "id": 2,
            "owner": 10,
          },
          "prevValues": [
            {
              "_0_version": "01",
              "desc": "bar",
              "id": 2,
              "owner": 10,
            },
          ],
          "rowKey": {
            "id": 2,
          },
          "table": "issues",
        },
      ]
    `);

    s1.destroy();
    s2.destroy();
  });

  test('truncate', () => {
    const {version} = s.current();

    expect(version).toBe('01');

    replicator.processTransaction('07', messages.truncate('users'));

    const diff = s.advance(tableSpecs, allTableNames);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('07');
    expect(diff.changes).toBe(1);

    expect(() => [...diff]).toThrowError(ResetPipelinesSignal);
  });

  test('permissions change', () => {
    const {version} = s.current();

    expect(version).toBe('01');

    replicator.processTransaction(
      '07',
      messages.update('my_app.permissions', {
        lock: 1,
        permissions: '{"tables":{}}',
        hash: '12345',
      }),
    );

    const diff = s.advance(tableSpecs, allTableNames);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('07');
    expect(diff.changes).toBe(1);

    expect(() => [...diff]).toThrowError(ResetPipelinesSignal);
  });

  test('changelog iterator cleaned up on aborted iteration', () => {
    const {version} = s.current();

    expect(version).toBe('01');

    replicator.processTransaction('07', messages.insert('comments', {id: 1}));

    const diff = s.advance(tableSpecs, allTableNames);
    let currStmts = 0;

    const abortError = new Error('aborted iteration');
    try {
      for (const change of diff) {
        expect(change).toEqual({
          nextValue: {
            ['_0_version']: '07',
            desc: null,
            id: 1,
          },
          prevValues: [],
          rowKey: {id: 1},
          table: 'comments',
        });
        currStmts = diff.curr.db.statementCache.size;
        throw abortError;
      }
    } catch (e) {
      expect(e).toBe(abortError);
    }

    // The Statement for the ChangeLog iteration should have been returned to the cache.
    expect(diff.curr.db.statementCache.size).toBe(currStmts + 1);
  });

  test('schema change diff iteration throws SchemaChangeError', () => {
    const {version} = s.current();

    expect(version).toBe('01');

    replicator.processTransaction(
      '07',
      messages.addColumn('comments', 'likes', {dataType: 'INT4', pos: 0}),
    );

    const diff = s.advance(tableSpecs, allTableNames);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('07');
    expect(diff.changes).toBe(1);

    expect(() => [...diff]).toThrow(ResetPipelinesSignal);
  });

  test('getRows filters out unique keys with NULL column values', () => {
    // This tests a critical performance optimization: when unique key columns
    // have NULL values, they must be filtered out of the OR query. Otherwise,
    // SQLite's MULTI-INDEX OR optimization fails and falls back to a full
    // table scan (hundreds of times slower on large tables).

    // Insert a user with a NULL handle
    replicator.processTransaction(
      '05',
      messages.insert('users', {id: 30, handle: null}),
    );

    const diff = s.advance(tableSpecs, allTableNames);
    expect(diff.curr.version).toBe('05');

    // Spy on the statement cache to see what queries are generated
    const getSpy = vi.spyOn(diff.prev.db.statementCache, 'get');

    // Consume the diff - this will call getRows for the user with NULL handle
    const changes = [...diff];
    expect(changes).toHaveLength(1);

    // Find the getRows query (SELECT from users with WHERE clause)
    const getRowsCalls = getSpy.mock.calls.filter(
      call =>
        typeof call[0] === 'string' &&
        call[0].includes('FROM "users"') &&
        call[0].includes('WHERE'),
    );

    // Should have made exactly one query for the users table
    expect(getRowsCalls).toHaveLength(1);

    // Snapshot the entire query - it should only have "id"=? in WHERE,
    // not "handle"=? since handle is NULL
    const [query] = getRowsCalls[0]!;
    expect(query).toBe(
      'SELECT "id","handle","_0_version" FROM "users" WHERE "id"=?',
    );

    // Keep this lookup index-driven. A nullable alternate key must not enter
    // the batched OR lookup.
    const plan = diff.prev.db.db
      .prepare(`EXPLAIN QUERY PLAN ${query}`)
      .all<{detail: string}>(30)
      .map(row => row.detail)
      .join('\n');
    expect(plan).toMatch(/SEARCH users USING/);
    expect(plan).not.toMatch(/\bSCAN users\b/);

    getSpy.mockRestore();
  });

  test('unobserved tables are skipped without row lookups', () => {
    const {version} = s.current();
    expect(version).toBe('01');

    replicator.processTransaction(
      '07',
      messages.insert('users', {id: 'u1', handle: 'alice'}),
      messages.insert('issues', {id: 1, desc: 'bug', owner: 1}),
    );

    // Only observe 'issues', users should be skipped
    const observed = new Set(['issues']);
    const diff = s.advance(tableSpecs, allTableNames, observed);
    expect(diff.changes).toBe(2);

    const prevSpy = vi.spyOn(diff.prev.db.statementCache, 'get');
    const currSpy = vi.spyOn(diff.curr.db.statementCache, 'get');

    const changes = [...diff];
    expect(changes).toHaveLength(1);
    expect(changes[0]?.table).toBe('issues');

    // Assert that no statement queries were executed for 'users'
    const prevUsersCalls = prevSpy.mock.calls.filter(
      call => typeof call[0] === 'string' && call[0].includes('"users"'),
    );
    const currUsersCalls = currSpy.mock.calls.filter(
      call => typeof call[0] === 'string' && call[0].includes('"users"'),
    );
    expect(prevUsersCalls).toHaveLength(0);
    expect(currUsersCalls).toHaveLength(0);

    // Assert that queries WERE executed for the observed 'issues' table
    const currIssuesCalls = currSpy.mock.calls.filter(
      call => typeof call[0] === 'string' && call[0].includes('"issues"'),
    );
    expect(currIssuesCalls.length).toBeGreaterThan(0);

    prevSpy.mockRestore();
    currSpy.mockRestore();
  });

  test('permissions change is observed even when not in observedTables', () => {
    const {version} = s.current();
    expect(version).toBe('01');

    replicator.processTransaction(
      '07',
      messages.update('my_app.permissions', {
        lock: 1,
        permissions: '{"tables":{}}',
        hash: '12345',
      }),
      messages.insert('issues', {id: 1, desc: 'bug', owner: 1}),
    );

    const observed = new Set(['issues']);
    const diff = s.advance(tableSpecs, allTableNames, observed);
    expect(() => [...diff]).toThrowError(ResetPipelinesSignal);
  });

  describe('snapshot row cache', () => {
    // Counts the row lookups (i.e. statements other than the change log
    // iteration) executed on the snapshots of a diff.
    function rowLookups(spy: {mock: {calls: unknown[][]}}) {
      return spy.mock.calls.filter(
        call => typeof call[0] === 'string' && !call[0].includes('changeLog2'),
      ).length;
    }

    test('row reads are shared across Snapshotters', () => {
      const cache = new SnapshotRowCache(1000);
      const s1 = new Snapshotter(
        lc,
        dbFile.path,
        {appID: 'my_app'},
        undefined,
        cache,
      ).init();
      const s2 = new Snapshotter(
        lc,
        dbFile.path,
        {appID: 'my_app'},
        undefined,
        cache,
      ).init();

      replicator.processTransaction(
        '09',
        messages.insert('issues', {id: 4, owner: 20}),
        messages.update('issues', {id: 1, owner: 10, desc: 'food'}),
        messages.delete('issues', {id: 3}),
        messages.update('users', {id: 10, handle: 'alicia'}),
        messages.delete('comments', {id: 123}), // never existed
      );

      const diff1 = s1.advance(tableSpecs, allTableNames);
      const changes1 = [...diff1];
      expect(changes1).toMatchInlineSnapshot(`
        [
          {
            "nextValue": {
              "_0_version": "09",
              "desc": null,
              "id": 4,
              "owner": 20,
            },
            "prevValues": [],
            "rowKey": {
              "id": 4,
            },
            "table": "issues",
          },
          {
            "nextValue": {
              "_0_version": "09",
              "desc": "food",
              "id": 1,
              "owner": 10,
            },
            "prevValues": [
              {
                "_0_version": "01",
                "desc": "foo",
                "id": 1,
                "owner": 10,
              },
            ],
            "rowKey": {
              "id": 1,
            },
            "table": "issues",
          },
          {
            "nextValue": null,
            "prevValues": [
              {
                "_0_version": "01",
                "desc": "baz",
                "id": 3,
                "owner": 20,
              },
            ],
            "rowKey": {
              "id": 3,
            },
            "table": "issues",
          },
          {
            "nextValue": {
              "_0_version": "09",
              "handle": "alicia",
              "id": 10,
            },
            "prevValues": [
              {
                "_0_version": "01",
                "handle": "alice",
                "id": 10,
              },
            ],
            "rowKey": {
              "id": 10,
            },
            "table": "users",
          },
        ]
      `);
      // 2 reads per set (curr.getRow + prev.getRows), 1 per delete
      // (prev.getRow). The missing "comments" row is not cached.
      expect(cache.stats()).toEqual({hits: 0, misses: 8, size: 7});

      // The second Snapshotter's reads are all served from the cache.
      const diff2 = s2.advance(tableSpecs, allTableNames);
      expect(diff2.prev.version).toBe('01');
      expect(diff2.curr.version).toBe('09');
      const prevSpy = vi.spyOn(diff2.prev.db.statementCache, 'get');
      const currSpy = vi.spyOn(diff2.curr.db.statementCache, 'get');

      const changes2 = [...diff2];
      expect(changes2).toEqual(changes1);
      expect(cache.stats()).toEqual({hits: 7, misses: 9, size: 7});
      expect(rowLookups(prevSpy)).toBe(1); // the missing "comments" row
      expect(rowLookups(currSpy)).toBe(0);

      // Rows produced by the diff are not shared with the cached SQLite rows.
      expect(changes2[0].nextValue).not.toBe(changes1[0].nextValue);
      expect(changes2[1].prevValues[0]).not.toBe(changes1[1].prevValues[0]);

      // Re-iterating a diff is likewise served from the cache.
      expect([...diff1]).toEqual(changes1);
      expect(cache.stats()).toEqual({hits: 14, misses: 10, size: 7});

      prevSpy.mockRestore();
      currSpy.mockRestore();
      s1.destroy();
      s2.destroy();
    });

    test('sharing across Snapshotters at different curr versions', () => {
      const cache = new SnapshotRowCache(1000);
      const s1 = new Snapshotter(
        lc,
        dbFile.path,
        {appID: 'my_app'},
        undefined,
        cache,
      ).init();
      const s2 = new Snapshotter(
        lc,
        dbFile.path,
        {appID: 'my_app'},
        undefined,
        cache,
      ).init();

      replicator.processTransaction(
        '09',
        messages.insert('issues', {id: 4, owner: 20}),
        messages.update('issues', {id: 1, owner: 10, desc: 'food'}),
        messages.delete('issues', {id: 3}),
        messages.update('users', {id: 10, handle: 'alicia'}),
      );

      const changes1 = [...s1.advance(tableSpecs, allTableNames)];
      expect(cache.stats()).toEqual({hits: 0, misses: 7, size: 7});

      // s2 advances over a range that includes a subsequent transaction.
      replicator.processTransaction('0b', messages.insert('comments', {id: 1}));

      const diff2 = s2.advance(tableSpecs, allTableNames);
      expect(diff2.prev.version).toBe('01');
      expect(diff2.curr.version).toBe('0b');
      const changes2 = [...diff2];
      expect(changes2).toEqual([
        ...changes1,
        {
          nextValue: {['_0_version']: '0b', desc: null, id: 1},
          prevValues: [],
          rowKey: {id: 1},
          table: 'comments',
        },
      ]);
      // Hits: the new values of the 3 "issues" and "users" rows, and the
      //       previous values of the 3 "issues" rows (whose only unique key
      //       is the primary key).
      // Misses: the previous value of the "users" row, whose table has an
      //         additional unique key (and thus depends on the curr version),
      //         and the 2 reads for the new "comments" row.
      expect(cache.stats()).toEqual({hits: 6, misses: 10, size: 10});

      s1.destroy();
      s2.destroy();
    });

    test('replaying a diff after advancing throws despite cache hits', () => {
      const cache = new SnapshotRowCache(1000);
      const s = new Snapshotter(
        lc,
        dbFile.path,
        {appID: 'my_app'},
        undefined,
        cache,
      ).init();

      try {
        replicator.processTransaction(
          '02',
          messages.update('issues', {id: 1, owner: 10, desc: 'updated'}),
        );
        const diff = s.advance(tableSpecs, allTableNames);
        expect([...diff]).toHaveLength(1);

        replicator.processTransaction(
          '03',
          messages.insert('comments', {id: 1}),
        );
        s.advance(tableSpecs, allTableNames);

        // Every read of the replay would be a cache hit.
        expect(() => [...diff]).toThrow(InvalidDiffError);
      } finally {
        s.destroy();
      }
    });

    test('an invalid diff does not poison the cache for valid diffs', () => {
      const cache = new SnapshotRowCache(1000);
      const s1 = new Snapshotter(
        lc,
        dbFile.path,
        {appID: 'my_app'},
        undefined,
        cache,
      ).init();
      const s2 = new Snapshotter(
        lc,
        dbFile.path,
        {appID: 'my_app'},
        undefined,
        cache,
      ).init();

      try {
        replicator.processTransaction(
          '02',
          messages.update('issues', {id: 1, owner: 10, desc: 'updated'}),
        );
        const stale = s1.advance(tableSpecs, allTableNames);
        replicator.processTransaction(
          '03',
          messages.update('issues', {id: 1, owner: 10, desc: 'again'}),
        );
        // Advancing s1 again moves stale.prev's connection to 03.
        s1.advance(tableSpecs, allTableNames);
        expect(() => [...stale]).toThrow(InvalidDiffError);

        // s2's diff from 01 is valid, and must not be served the row that
        // the stale diff read from its advanced connection.
        const diff = s2.advance(tableSpecs, allTableNames);
        expect(diff.prev.version).toBe('01');
        expect([...diff]).toEqual([
          {
            table: 'issues',
            rowKey: {id: 1},
            prevValues: [{id: 1, owner: 10, desc: 'foo', _0_version: '01'}],
            nextValue: {id: 1, owner: 10, desc: 'again', _0_version: '03'},
          },
        ]);
      } finally {
        s1.destroy();
        s2.destroy();
      }
    });

    test.each([false, true])(
      'update followed by delete across Snapshotters (shared cache: %s)',
      sharedCache => {
        const cache = sharedCache ? new SnapshotRowCache() : undefined;
        const s1 = new Snapshotter(
          lc,
          dbFile.path,
          {appID: 'my_app'},
          undefined,
          cache,
        ).init();
        const s2 = new Snapshotter(
          lc,
          dbFile.path,
          {appID: 'my_app'},
          undefined,
          cache,
        ).init();

        try {
          // issues has only a primary key, so getRows() for the update and
          // getRow() for the delete produce the same SQL and p:01 cache tag.
          const original = {id: 1, owner: 10, desc: 'foo', _0_version: '01'};
          replicator.processTransaction(
            '02',
            messages.update('issues', {id: 1, owner: 10, desc: 'updated'}),
          );
          const update = s1.advance(tableSpecs, allTableNames);
          expect(update.prev.version).toBe('01');
          expect(update.curr.version).toBe('02');
          expect([...update]).toEqual([
            {
              table: 'issues',
              rowKey: {id: 1},
              prevValues: [original],
              nextValue: {...original, desc: 'updated', _0_version: '02'},
            },
          ]);

          // Leave s2 at 01 until the updated row has been deleted at 03.
          // Its previous-row read must return a row, not the array cached
          // while iterating s1's update. Currently this throws InvalidDiffError.
          replicator.processTransaction(
            '03',
            messages.delete('issues', {id: 1}),
          );
          const deletion = s2.advance(tableSpecs, allTableNames);
          expect(deletion.prev.version).toBe('01');
          expect(deletion.curr.version).toBe('03');
          expect([...deletion]).toEqual([
            {
              table: 'issues',
              rowKey: {id: 1},
              prevValues: [original],
              nextValue: null,
            },
          ]);
        } finally {
          s1.destroy();
          s2.destroy();
        }
      },
    );

    test('diffs that include a table-wide op bypass the cache', () => {
      const cache = new SnapshotRowCache(1000);
      const s1 = new Snapshotter(
        lc,
        dbFile.path,
        {appID: 'my_app'},
        undefined,
        cache,
      ).init();

      replicator.processTransaction(
        '09',
        messages.insert('issues', {id: 4, owner: 20}),
      );
      replicator.processTransaction('0b', messages.truncate('users'));

      const diff = s1.advance(tableSpecs, allTableNames);
      expect(diff.changes).toBe(2);

      const changes: Change[] = [];
      expect(() => {
        for (const change of diff) {
          changes.push(change);
        }
      }).toThrowError(ResetPipelinesSignal);

      // The change before the truncate was still read (directly).
      expect(changes).toHaveLength(1);
      expect(cache.stats()).toEqual({hits: 0, misses: 0, size: 0});

      s1.destroy();
    });

    test('a Snapshotter without a cache reads directly', () => {
      replicator.processTransaction(
        '09',
        messages.insert('issues', {id: 4, owner: 20}),
      );
      const diff = s.advance(tableSpecs, allTableNames);
      const currSpy = vi.spyOn(diff.curr.db.statementCache, 'get');
      const prevSpy = vi.spyOn(diff.prev.db.statementCache, 'get');
      expect([...diff]).toHaveLength(1);
      expect(rowLookups(currSpy)).toBe(1);
      expect(rowLookups(prevSpy)).toBe(1);
      currSpy.mockRestore();
      prevSpy.mockRestore();
    });
  });
});
