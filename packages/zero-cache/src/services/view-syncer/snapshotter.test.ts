import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.js';
import {listTables} from '../../db/lite-tables.js';
import type {LiteAndZqlSpec} from '../../db/specs.js';
import {DbFile, expectTables} from '../../test/lite.js';
import {initChangeLog} from '../replicator/schema/change-log.js';
import {initReplicationState} from '../replicator/schema/replication-state.js';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.js';
import {setSpecs} from './pipeline-driver.js';
import {
  InvalidDiffError,
  ResetPipelinesSignal,
  Snapshotter,
} from './snapshotter.js';

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
        CREATE TABLE "zero.schemaVersions" (
          "lock"                INTEGER PRIMARY KEY,
          "minSupportedVersion" INTEGER,
          "maxSupportedVersion" INTEGER,
          _0_version            TEXT NOT NULL
        );
        INSERT INTO "zero.schemaVersions" ("lock", "minSupportedVersion", "maxSupportedVersion", _0_version)    
          VALUES (1, 1, 1, '00');  
        CREATE TABLE issues(id INTEGER PRIMARY KEY, owner INTEGER, desc TEXT, ignore TEXT, _0_version TEXT NOT NULL);
        CREATE TABLE users(id INTEGER PRIMARY KEY, handle TEXT, ignore TEXT, _0_version TEXT NOT NULL);
        CREATE TABLE comments(id INTEGER PRIMARY KEY, desc TEXT, ignore TEXT, _0_version TEXT NOT NULL);

        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(1, 10, 'foo', 'zzz', '00');
        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(2, 10, 'bar', 'xyz', '00');
        INSERT INTO issues(id, owner, desc, ignore, _0_version) VALUES(3, 20, 'baz', 'yyy', '00');

        INSERT INTO users(id, handle, ignore, _0_version) VALUES(10, 'alice', 'vvv', '00');
        INSERT INTO users(id, handle, ignore, _0_version) VALUES(20, 'bob', 'vxv', '00');
      `,
    );
    initReplicationState(db, ['zero_data'], '01');
    initChangeLog(db);

    // The 'ignore' column should not show up in the diffs.
    const tables = listTables(db);
    tables.forEach(t => delete (t.columns as Record<string, unknown>).ignore);
    tableSpecs = new Map();
    setSpecs(tables, tableSpecs);

    replicator = fakeReplicator(lc, db);
    s = new Snapshotter(lc, dbFile.path).init();
  });

  afterEach(() => {
    s.destroy();
    dbFile.delete();
  });

  test('initial snapshot', () => {
    const {db, version, schemaVersions} = s.current();

    expect(version).toBe('00');
    expect(schemaVersions).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 1,
    });
    expectTables(db.db, {
      issues: [
        {id: 1, owner: 10, desc: 'foo', ignore: 'zzz', ['_0_version']: '00'},
        {id: 2, owner: 10, desc: 'bar', ignore: 'xyz', ['_0_version']: '00'},
        {id: 3, owner: 20, desc: 'baz', ignore: 'yyy', ['_0_version']: '00'},
      ],
      users: [
        {id: 10, handle: 'alice', ignore: 'vvv', ['_0_version']: '00'},
        {id: 20, handle: 'bob', ignore: 'vxv', ['_0_version']: '00'},
      ],
    });
  });

  test('empty diff', () => {
    const {version} = s.current();

    expect(version).toBe('00');

    const diff = s.advance(tableSpecs);
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
    expect(s.current().version).toBe('00');
    expect(s.current().schemaVersions).toEqual({
      minSupportedVersion: 1,
      maxSupportedVersion: 1,
    });

    replicator.processTransaction(
      '07',
      zeroMessages.insert('schemaVersions', {
        lock: true,
        minSupportedVersion: 1,
        maxSupportedVersion: 2,
      }),
    );

    const diff = s.advance(tableSpecs);
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
            "lock": 1,
            "maxSupportedVersion": 2,
            "minSupportedVersion": 1,
          },
          "prevValue": {
            "_0_version": "00",
            "lock": 1,
            "maxSupportedVersion": 1,
            "minSupportedVersion": 1,
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

    replicator.processTransaction(
      '09',
      messages.insert('issues', {id: 4, owner: 20}),
      messages.update('issues', {id: 1, owner: 10, desc: 'food'}),
      messages.update('issues', {id: 5, owner: 10, desc: 'bard'}, {id: 2}),
      messages.delete('issues', {id: 3}),
    );

    const diff1 = s1.advance(tableSpecs);
    expect(diff1.prev.version).toBe('00');
    expect(diff1.curr.version).toBe('01');
    expect(diff1.changes).toBe(5); // The key update results in a del(old) + set(new).

    expect([...diff1]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "01",
            "desc": "food",
            "id": 1,
            "owner": 10,
          },
          "prevValue": {
            "_0_version": "00",
            "desc": "foo",
            "id": 1,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "bar",
            "id": 2,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "baz",
            "id": 3,
            "owner": 20,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "desc": null,
            "id": 4,
            "owner": 20,
          },
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "01",
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
            "_0_version": "01",
            "desc": "food",
            "id": 1,
            "owner": 10,
          },
          "prevValue": {
            "_0_version": "00",
            "desc": "foo",
            "id": 1,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "bar",
            "id": 2,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "baz",
            "id": 3,
            "owner": 20,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "desc": null,
            "id": 4,
            "owner": 20,
          },
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "01",
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
    expect(diff2.prev.version).toBe('01');
    expect(diff2.curr.version).toBe('09');
    expect(diff2.changes).toBe(3);

    expect([...diff2]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "09",
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
            "_0_version": "01",
            "desc": null,
            "id": 4,
            "owner": 20,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "01",
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
    expect(diff3.prev.version).toBe('00');
    expect(diff3.curr.version).toBe('09');
    expect(diff3.changes).toBe(5);
    expect([...diff3]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "01",
            "desc": "food",
            "id": 1,
            "owner": 10,
          },
          "prevValue": {
            "_0_version": "00",
            "desc": "foo",
            "id": 1,
            "owner": 10,
          },
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "desc": "baz",
            "id": 3,
            "owner": 20,
          },
          "table": "issues",
        },
        {
          "nextValue": {
            "_0_version": "09",
            "desc": "bard",
            "id": 2,
            "owner": 10,
          },
          "prevValue": {
            "_0_version": "00",
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

    expect(version).toBe('00');

    replicator.processTransaction('07', messages.truncate('users'));

    const diff = s.advance(tableSpecs);
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(1);

    expect(() => [...diff]).toThrowError(ResetPipelinesSignal);
  });

  test('changelog iterator cleaned up on aborted iteration', () => {
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processTransaction('07', messages.insert('comments', {id: 1}));

    const diff = s.advance(tableSpecs);
    let currStmts = 0;

    const abortError = new Error('aborted iteration');
    try {
      for (const change of diff) {
        expect(change).toEqual({
          nextValue: {
            ['_0_version']: '01',
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

    expect(version).toBe('00');

    replicator.processTransaction(
      '07',
      messages.addColumn('comments', 'likes', {dataType: 'INT4', pos: 0}),
    );

    const diff = s.advance(tableSpecs);
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(1);

    expect(() => [...diff]).toThrow(ResetPipelinesSignal);
  });
});
