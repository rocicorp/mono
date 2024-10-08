import {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {DbFile, expectTables} from 'zero-cache/src/test/lite.js';
import {MessageProcessor} from '../replicator/incremental-sync.js';
import {initChangeLog} from '../replicator/schema/change-log.js';
import {initReplicationState} from '../replicator/schema/replication-state.js';
import {
  createMessageProcessor,
  ReplicationMessages,
} from '../replicator/test-utils.js';
import {InvalidDiffError, Snapshotter} from './snapshotter.js';

describe('view-syncer/snapshotter', () => {
  let lc: LogContext;
  let dbFile: DbFile;
  let replicator: MessageProcessor;

  beforeEach(() => {
    lc = createSilentLogContext();
    dbFile = new DbFile('snapshotter_test');
    const db = dbFile.connect(lc);
    db.pragma('journal_mode = WAL');
    db.exec(
      `
        CREATE TABLE "zero.schemaVersions" (
          "lock"                INTEGER PRIMARY KEY,
          "minSupportedVersion" INTEGER,
          "maxSupportedVersion" INTEGER,
          _0_version            TEXT NOT NULL
        );
        INSERT INTO "zero.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
          VALUES (1, 1, 1, '00');  
        CREATE TABLE issues(id INTEGER PRIMARY KEY, owner INTEGER, desc TEXT, _0_version TEXT NOT NULL);
        CREATE TABLE users(id INTEGER PRIMARY KEY, handle TEXT, _0_version TEXT NOT NULL);
        CREATE TABLE comments(id INTEGER PRIMARY KEY, desc TEXT, _0_version TEXT NOT NULL);

        INSERT INTO issues(id, owner, desc, _0_version) VALUES(1, 10, 'foo', '00');
        INSERT INTO issues(id, owner, desc, _0_version) VALUES(2, 10, 'bar', '00');
        INSERT INTO issues(id, owner, desc, _0_version) VALUES(3, 20, 'baz', '00');

        INSERT INTO users(id, handle, _0_version) VALUES(10, 'alice', '00');
        INSERT INTO users(id, handle, _0_version) VALUES(20, 'bob', '00');
      `,
    );
    initReplicationState(db, ['zero_data'], '01');
    initChangeLog(db);

    replicator = createMessageProcessor(db);
  });

  afterEach(async () => {
    await dbFile.unlink();
  });

  test('initial snapshot', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    const {db, version, schemaVersions} = s.current();

    expect(version).toBe('00');
    expect(schemaVersions).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 1,
    });
    expectTables(db.db, {
      issues: [
        {id: 1, owner: 10, desc: 'foo', ['_0_version']: '00'},
        {id: 2, owner: 10, desc: 'bar', ['_0_version']: '00'},
        {id: 3, owner: 20, desc: 'baz', ['_0_version']: '00'},
      ],
      users: [
        {id: 10, handle: 'alice', ['_0_version']: '00'},
        {id: 20, handle: 'bob', ['_0_version']: '00'},
      ],
    });
  });

  test('empty diff', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    const {version} = s.current();

    expect(version).toBe('00');

    const diff = s.advance();
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('00');
    expect(diff.changes).toBe(0);

    expect([...diff]).toEqual([]);
  });

  const messages = new ReplicationMessages({
    issues: 'id',
    users: 'id',
    comments: 'id',
  });

  const zeroMessages = new ReplicationMessages(
    {
      schemaVersions: 'lock',
    },
    'zero',
  );

  test('schemaVersions change', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    expect(s.current().version).toBe('00');
    expect(s.current().schemaVersions).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 1,
    });

    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, [
      'data',
      zeroMessages.insert('schemaVersions', {
        lock: '1',
        minSupportedVersion: 1,
        maxSupportedVersion: 2,
      }),
    ]);
    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '07'},
    ]);

    const diff = s.advance();
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(1);

    expect(s.current().version).toBe('01');
    expect(s.current().schemaVersions).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 2,
    });

    expect([...diff]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "01",
            "lock": 1n,
            "maxSupportedVersion": 2n,
            "minSupportedVersion": 1n,
          },
          "prevValue": {
            "_0_version": "00",
            "lock": 1n,
            "maxSupportedVersion": 1n,
            "minSupportedVersion": 1n,
          },
          "table": "zero.schemaVersions",
        },
      ]
    `);
  });

  test('concurrent snapshot diffs', () => {
    const s1 = new Snapshotter(lc, dbFile.path).init();
    const s2 = new Snapshotter(lc, dbFile.path).init();

    expect(s1.current().version).toBe('00');
    expect(s2.current().version).toBe('00');

    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, [
      'data',
      messages.insert('issues', {id: 4, owner: 20}),
    ]);
    replicator.processMessage(lc, [
      'data',
      messages.update('issues', {id: 1, owner: 10, desc: 'food'}),
    ]);
    replicator.processMessage(lc, [
      'data',
      messages.update('issues', {id: 5, owner: 10, desc: 'bard'}, {id: 2}),
    ]);
    replicator.processMessage(lc, ['data', messages.delete('issues', {id: 3})]);
    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '09'},
    ]);

    const diff1 = s1.advance();
    expect(diff1.prev.version).toBe('00');
    expect(diff1.curr.version).toBe('01');
    expect(diff1.changes).toBe(5); // The key update results in a del(old) + set(new).

    expect([...diff1]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "01",
            "desc": "food",
            "id": 1n,
            "owner": 10n,
          },
          "prevValue": {
            "_0_version": "00",
            "desc": "foo",
            "id": 1n,
            "owner": 10n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "bar",
            "id": 2n,
            "owner": 10n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "baz",
            "id": 3n,
            "owner": 20n,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "desc": null,
            "id": 4n,
            "owner": 20n,
          },
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "desc": "bard",
            "id": 5n,
            "owner": 10n,
          },
          "prevValue": null,
          "table": "issues",
        },
      ]
    `);

    // Diff should be reusable as long as advance() hasn't been called.
    expect([...diff1]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "01",
            "desc": "food",
            "id": 1n,
            "owner": 10n,
          },
          "prevValue": {
            "_0_version": "00",
            "desc": "foo",
            "id": 1n,
            "owner": 10n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "bar",
            "id": 2n,
            "owner": 10n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "baz",
            "id": 3n,
            "owner": 20n,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "desc": null,
            "id": 4n,
            "owner": 20n,
          },
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "desc": "bard",
            "id": 5n,
            "owner": 10n,
          },
          "prevValue": null,
          "table": "issues",
        },
      ]
    `);

    // Replicate a second transaction
    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, ['data', messages.delete('issues', {id: 4})]);
    replicator.processMessage(lc, [
      'data',
      messages.update('issues', {id: 2, owner: 10, desc: 'bard'}, {id: 5}),
    ]);

    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '0d'},
    ]);

    const diff2 = s1.advance();
    expect(diff2.prev.version).toBe('01');
    expect(diff2.curr.version).toBe('09');
    expect(diff2.changes).toBe(3);

    expect([...diff2]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "bard",
            "id": 2n,
            "owner": 10n,
          },
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "01",
            "desc": null,
            "id": 4n,
            "owner": 20n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "01",
            "desc": "bard",
            "id": 5n,
            "owner": 10n,
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
    const diff3 = s2.advance();
    expect(diff3.prev.version).toBe('00');
    expect(diff3.curr.version).toBe('09');
    expect(diff3.changes).toBe(5);
    expect([...diff3]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "01",
            "desc": "food",
            "id": 1n,
            "owner": 10n,
          },
          "prevValue": {
            "_0_version": "00",
            "desc": "foo",
            "id": 1n,
            "owner": 10n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "baz",
            "id": 3n,
            "owner": 20n,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "bard",
            "id": 2n,
            "owner": 10n,
          },
          "prevValue": {
            "_0_version": "00",
            "desc": "bar",
            "id": 2n,
            "owner": 10n,
          },
          "table": "issues",
        },
      ]
    `);
  });

  test('noop-truncate diff', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, ['data', messages.truncate('comments')]);
    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '07'},
    ]);

    const diff = s.advance();
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(1);

    expect([...diff]).toEqual([]);
  });

  test('truncate diff', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, ['data', messages.truncate('users')]);
    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '07'},
    ]);

    const diff = s.advance();
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(1);

    expect([...diff]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "alice",
            "id": 10n,
          },
          "table": "users",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "bob",
            "id": 20n,
          },
          "table": "users",
        },
      ]
    `);
  });

  test('consecutive truncates', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, ['data', messages.truncate('issues')]);
    replicator.processMessage(lc, ['data', messages.truncate('users')]);
    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '08'},
    ]);

    const diff = s.advance();
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(2);

    expect([...diff]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "foo",
            "id": 1n,
            "owner": 10n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "bar",
            "id": 2n,
            "owner": 10n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "baz",
            "id": 3n,
            "owner": 20n,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "alice",
            "id": 10n,
          },
          "table": "users",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "bob",
            "id": 20n,
          },
          "table": "users",
        },
      ]
    `);
  });

  test('truncate followed by inserts into same table', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, ['data', messages.truncate('users')]);
    replicator.processMessage(lc, [
      'data',
      messages.insert('users', {id: 20, handle: 'robert'}),
    ]);
    replicator.processMessage(lc, [
      'data',
      messages.insert('users', {id: 30, handle: 'candice'}),
    ]);
    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '09'},
    ]);

    const diff = s.advance();
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(3);

    expect([...diff]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "alice",
            "id": 10n,
          },
          "table": "users",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "bob",
            "id": 20n,
          },
          "table": "users",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "handle": "robert",
            "id": 20n,
          },
          "prevValue": null,
          "table": "users",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "handle": "candice",
            "id": 30n,
          },
          "prevValue": null,
          "table": "users",
        },
      ]
    `);
  });

  test('changelog iterator cleaned up on aborted iteration', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, [
      'data',
      messages.insert('comments', {id: 1}),
    ]);
    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '07'},
    ]);

    const diff = s.advance();
    let currStmts = 0;

    const abortError = new Error('aborted iteration');
    try {
      for (const change of diff) {
        expect(change).toEqual({
          nextValue: {
            ['_0_version']: '01',
            desc: null,
            id: 1n,
          },
          prevValue: null,
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

  test('truncate iterator cleaned up on aborted iteration', () => {
    const s = new Snapshotter(lc, dbFile.path).init();
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, ['begin', messages.begin()]);
    replicator.processMessage(lc, ['data', messages.truncate('users')]);
    replicator.processMessage(lc, [
      'commit',
      messages.commit(),
      {watermark: '07'},
    ]);

    const diff = s.advance();
    let currStmts = 0;
    let prevStmts = 0;

    const abortError = new Error('aborted iteration');
    try {
      for (const change of diff) {
        expect(change).toEqual({
          nextValue: null,
          prevValue: {
            ['_0_version']: '00',
            handle: 'alice',
            id: 10n,
          },
          table: 'users',
        });
        currStmts = diff.curr.db.statementCache.size;
        prevStmts = diff.prev.db.statementCache.size;
        throw abortError;
      }
    } catch (e) {
      expect(e).toBe(abortError);
    }

    // The Statements for both the ChangeLog (curr) and truncated-row (prev)
    // iterations should have been returned to the cache.
    expect(diff.curr.db.statementCache.size).toBe(currStmts + 1);
    expect(diff.prev.db.statementCache.size).toBe(prevStmts + 1);
  });
});
