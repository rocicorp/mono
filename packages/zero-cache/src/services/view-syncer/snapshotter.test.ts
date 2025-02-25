import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {computeZqlSpecs} from '../../db/lite-tables.ts';
import type {LiteAndZqlSpec} from '../../db/specs.ts';
import {DbFile, expectTables} from '../../test/lite.ts';
import {initChangeLog} from '../replicator/schema/change-log.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {
  InvalidDiffError,
  ResetPipelinesSignal,
  Snapshotter,
} from './snapshotter.ts';

describe('view-syncer/snapshotter', () => {
  let lc: LogContext;
  let dbFile: DbFile;
  let replicator: FakeReplicator;
  let tableSpecs: Map<string, LiteAndZqlSpec>;
  let s: Snapshotter;

  beforeEach(() => {
    lc = createSilentLogContext();
    dbFile = new DbFile('snapshotter_test');
    const db = dbFile.connect(lc);
    db.pragma('journal_mode = WAL2');
    db.exec(
      `
        CREATE TABLE "my_app.permissions" (
          "lock"        INT PRIMARY KEY,
          "permissions" JSON,
          "hash"        TEXT,
          _0_version    TEXT NOT NULL
        );
        INSERT INTO "my_app.permissions" ("lock", "_0_version") VALUES (1, '01');
        CREATE TABLE "my_app.schemaVersions" (
          "lock"                INT PRIMARY KEY,
          "minSupportedVersion" INTEGER,
          "maxSupportedVersion" INTEGER,
          _0_version            TEXT NOT NULL
        );
        INSERT INTO "my_app.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
          VALUES (1, 1, 1, '01');  
        CREATE TABLE issues(id INT PRIMARY KEY, owner INTEGER, desc TEXT, ignore UNSUPPORTED_TYPE, _0_version TEXT NOT NULL);
        CREATE TABLE users(id INT PRIMARY KEY, handle TEXT, ignore UNSUPPORTED_TYPE, _0_version TEXT NOT NULL);
        CREATE TABLE comments(id INT PRIMARY KEY, desc TEXT, ignore UNSUPPORTED_TYPE, _0_version TEXT NOT NULL);

        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(1, 10, 'foo', 'zzz', '01');
        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(2, 10, 'bar', 'xyz', '01');
        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(3, 20, 'baz', 'yyy', '01');

        INSERT INTO users(id, handle, ignore, _0_version) VALUES(10, 'alice', 'vvv', '01');
        INSERT INTO users(id, handle, ignore, _0_version) VALUES(20, 'bob', 'vxv', '01');
      `,
    );
    initReplicationState(db, ['zero_data'], '01');
    initChangeLog(db);

    tableSpecs = computeZqlSpecs(lc, db);

    replicator = fakeReplicator(lc, db);
    s = new Snapshotter(lc, dbFile.path, 'my_app').init();
  });

  afterEach(() => {
    s.destroy();
    dbFile.delete();
  });

  test('initial snapshot', () => {
    const {db, version, schemaVersions} = s.current();

    expect(version).toBe('01');
    expect(schemaVersions).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 1,
    });
    expectTables(db.db, {
      issues: [
        {id: 1, owner: 10, desc: 'foo', ignore: 'zzz', ['_0_version']: '01'},
        {id: 2, owner: 10, desc: 'bar', ignore: 'xyz', ['_0_version']: '01'},
        {id: 3, owner: 20, desc: 'baz', ignore: 'yyy', ['_0_version']: '01'},
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

    const diff = s.advance(tableSpecs);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(0);

    expect([...diff]).toEqual([]);
  });

  const messages = new ReplicationMessages({
    issues: 'id',
    users: 'id',
    comments: 'id',
    ['my_app.permissions']: 'lock',
  });

  const appMessages = new ReplicationMessages(
    {
      schemaVersions: 'lock',
    },
    'my_app',
  );

  test('schemaVersions change', () => {
    expect(s.current().version).toBe('01');
    expect(s.current().schemaVersions).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 1,
    });

    replicator.processTransaction(
      '07',
      appMessages.update('schemaVersions', {
        lock: true,
        minSupportedVersion: 1,
        maxSupportedVersion: 2,
      }),
    );

    const diff = s.advance(tableSpecs);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('07');
    expect(diff.changes).toBe(1);

    expect(s.current().version).toBe('07');
    expect(s.current().schemaVersions).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 2,
    });

    expect([...diff]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "07",
            "lock": 1,
            "maxSupportedVersion": 2,
            "minSupportedVersion": 1,
          },
          "prevValue": {
            "_0_version": "01",
            "lock": 1,
            "maxSupportedVersion": 1,
            "minSupportedVersion": 1,
          },
          "table": "my_app.schemaVersions",
        },
      ]
    `);
  });

  test('concurrent snapshot diffs', () => {
    const s1 = new Snapshotter(lc, dbFile.path, 'my_app').init();
    const s2 = new Snapshotter(lc, dbFile.path, 'my_app').init();

    expect(s1.current().version).toBe('01');
    expect(s2.current().version).toBe('01');

    replicator.processTransaction(
      '09',
      messages.insert('issues', {id: 4, owner: 20}),
      messages.update('issues', {id: 1, owner: 10, desc: 'food'}),
      messages.update('issues', {id: 5, owner: 10, desc: 'bard'}, {id: 2}),
      messages.delete('issues', {id: 3}),
    );

    const diff1 = s1.advance(tableSpecs);
    expect(diff1.prev.version).toBe('01');
    expect(diff1.curr.version).toBe('09');
    expect(diff1.changes).toBe(5); // The key update results in a del(old) + set(new).

    expect([...diff1]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "food",
            "id": 1,
            "owner": 10,
          },
          "prevValue": {
            "_0_version": "01",
            "desc": "foo",
            "id": 1,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "01",
            "desc": "bar",
            "id": 2,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "01",
            "desc": "baz",
            "id": 3,
            "owner": 20,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": null,
            "id": 4,
            "owner": 20,
          },
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "bard",
            "id": 5,
            "owner": 10,
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
            "_0_version": "09",
            "desc": "food",
            "id": 1,
            "owner": 10,
          },
          "prevValue": {
            "_0_version": "01",
            "desc": "foo",
            "id": 1,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "01",
            "desc": "bar",
            "id": 2,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "01",
            "desc": "baz",
            "id": 3,
            "owner": 20,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": null,
            "id": 4,
            "owner": 20,
          },
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "bard",
            "id": 5,
            "owner": 10,
          },
          "prevValue": null,
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

    const diff2 = s1.advance(tableSpecs);
    expect(diff2.prev.version).toBe('09');
    expect(diff2.curr.version).toBe('0d');
    expect(diff2.changes).toBe(3);

    expect([...diff2]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "0d",
            "desc": "bard",
            "id": 2,
            "owner": 10,
          },
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "09",
            "desc": null,
            "id": 4,
            "owner": 20,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "09",
            "desc": "bard",
            "id": 5,
            "owner": 10,
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
    const diff3 = s2.advance(tableSpecs);
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
          "prevValue": {
            "_0_version": "01",
            "desc": "foo",
            "id": 1,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "01",
            "desc": "baz",
            "id": 3,
            "owner": 20,
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
          "prevValue": {
            "_0_version": "01",
            "desc": "bar",
            "id": 2,
            "owner": 10,
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

    const diff = s.advance(tableSpecs);
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

    const diff = s.advance(tableSpecs);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('07');
    expect(diff.changes).toBe(1);

    expect(() => [...diff]).toThrowError(ResetPipelinesSignal);
  });

  test('changelog iterator cleaned up on aborted iteration', () => {
    const {version} = s.current();

    expect(version).toBe('01');

    replicator.processTransaction('07', messages.insert('comments', {id: 1}));

    const diff = s.advance(tableSpecs);
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

  test('schema change diff iteration throws SchemaChangeError', () => {
    const {version} = s.current();

    expect(version).toBe('01');

    replicator.processTransaction(
      '07',
      messages.addColumn('comments', 'likes', {dataType: 'INT4', pos: 0}),
    );

    const diff = s.advance(tableSpecs);
    expect(diff.prev.version).toBe('01');
    expect(diff.curr.version).toBe('07');
    expect(diff.changes).toBe(1);

    expect(() => [...diff]).toThrow(ResetPipelinesSignal);
  });
});
