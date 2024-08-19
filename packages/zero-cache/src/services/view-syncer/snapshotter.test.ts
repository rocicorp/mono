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
    const db = dbFile.connect();
    db.pragma('journal_mode = WAL');
    db.exec(
      `
        CREATE TABLE issues(id INTEGER PRIMARY KEY, owner INTEGER, desc TEXT, _0_version TEXT NOT NULL);
        CREATE TABLE users(id INTEGER PRIMARY KEY, handle TEXT, _0_version TEXT NOT NULL);
        CREATE TABLE comments(id INTEGER PRIMARY KEY, handle TEXT, _0_version TEXT NOT NULL);

        INSERT INTO issues(id, owner, desc, _0_version) VALUES(1, 10, 'foo', '00');
        INSERT INTO issues(id, owner, desc, _0_version) VALUES(2, 10, 'bar', '00');
        INSERT INTO issues(id, owner, desc, _0_version) VALUES(3, 20, 'baz', '00');

        INSERT INTO users(id, handle, _0_version) VALUES(10, 'alice', '00');
        INSERT INTO users(id, handle, _0_version) VALUES(20, 'bob', '00');
      `,
    );
    initReplicationState(db, ['zero_data'], '0/1');
    initChangeLog(db);

    replicator = createMessageProcessor(db);
  });

  afterEach(async () => {
    await dbFile.unlink();
  });

  test('initial snapshot', () => {
    const s = new Snapshotter(lc, dbFile.path);
    const {db, version} = s.current();

    expect(version).toBe('00');
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
    const s = new Snapshotter(lc, dbFile.path);
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

  test('concurrent snapshot diffs', () => {
    const s1 = new Snapshotter(lc, dbFile.path);
    const s2 = new Snapshotter(lc, dbFile.path);

    expect(s1.current().version).toBe('00');
    expect(s2.current().version).toBe('00');

    replicator.processMessage(lc, '0/1', messages.begin());
    replicator.processMessage(
      lc,
      '0/1',
      messages.insert('issues', {id: 4, owner: 20}),
    );
    replicator.processMessage(
      lc,
      '0/1',
      messages.update('issues', {id: 1, owner: 10, desc: 'food'}),
    );
    replicator.processMessage(
      lc,
      '0/1',
      messages.update('issues', {id: 5, owner: 10, desc: 'bard'}, {id: 2}),
    );
    replicator.processMessage(lc, '0/1', messages.delete('issues', {id: 3}));
    replicator.processMessage(lc, '0/1', messages.commit('0/2'));

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
    replicator.processMessage(lc, '0/3', messages.begin());
    replicator.processMessage(lc, '0/3', messages.delete('issues', {id: 4}));
    replicator.processMessage(
      lc,
      '0/3',
      messages.update('issues', {id: 2, owner: 10, desc: 'bard'}, {id: 5}),
    );
    replicator.processMessage(lc, '0/3', messages.commit('0/4'));

    const diff2 = s1.advance();
    expect(diff2.prev.version).toBe('01');
    expect(diff2.curr.version).toBe('02');
    expect(diff2.changes).toBe(3);

    expect([...diff2]).toMatchInlineSnapshot(`
      [
        {
          "nextValue": {
            "_0_version": "02",
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

    // The diff for s2 goes straight from '00' to '02'.
    // This will coalesce multiple changes to a row, and can result in some noops,
    // (e.g. rows that return to their original state).
    const diff3 = s2.advance();
    expect(diff3.prev.version).toBe('00');
    expect(diff3.curr.version).toBe('02');
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
            "_0_version": "02",
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
        {
          "nextValue": null,
          "prevValue": null,
          "table": "issues",
        },
        {
          "nextValue": null,
          "prevValue": null,
          "table": "issues",
        },
      ]
    `);
  });

  test('noop-truncate diff', () => {
    const s = new Snapshotter(lc, dbFile.path);
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, '0/1', messages.begin());
    replicator.processMessage(lc, '0/1', messages.truncate('comments'));
    replicator.processMessage(lc, '0/1', messages.commit('0/2'));

    const diff = s.advance();
    expect(diff.prev.version).toBe('00');
    expect(diff.curr.version).toBe('01');
    expect(diff.changes).toBe(1);

    expect([...diff]).toEqual([]);
  });

  test('truncate diff', () => {
    const s = new Snapshotter(lc, dbFile.path);
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, '0/1', messages.begin());
    replicator.processMessage(lc, '0/1', messages.truncate('users'));
    replicator.processMessage(lc, '0/1', messages.commit('0/2'));

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
            "id": 10,
          },
          "table": "users",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "bob",
            "id": 20,
          },
          "table": "users",
        },
      ]
    `);
  });

  test('consecutive truncates', () => {
    const s = new Snapshotter(lc, dbFile.path);
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, '0/1', messages.begin());
    replicator.processMessage(lc, '0/1', messages.truncate('issues'));
    replicator.processMessage(lc, '0/1', messages.truncate('users'));
    replicator.processMessage(lc, '0/1', messages.commit('0/2'));

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
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "alice",
            "id": 10,
          },
          "table": "users",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "bob",
            "id": 20,
          },
          "table": "users",
        },
      ]
    `);
  });

  test('truncate followed by inserts into same table', () => {
    const s = new Snapshotter(lc, dbFile.path);
    const {version} = s.current();

    expect(version).toBe('00');

    replicator.processMessage(lc, '0/1', messages.begin());
    replicator.processMessage(lc, '0/1', messages.truncate('users'));
    replicator.processMessage(
      lc,
      '0/1',
      messages.insert('users', {id: 20, handle: 'robert'}),
    );
    replicator.processMessage(
      lc,
      '0/1',
      messages.insert('users', {id: 30, handle: 'candice'}),
    );
    replicator.processMessage(lc, '0/1', messages.commit('0/2'));

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
            "id": 10,
          },
          "table": "users",
        },
        {
          "nextValue": null,
          "prevValue": {
            "_0_version": "00",
            "handle": "bob",
            "id": 20,
          },
          "table": "users",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "handle": "robert",
            "id": 20,
          },
          "prevValue": null,
          "table": "users",
        },
        {
          "nextValue": {
            "_0_version": "01",
            "handle": "candice",
            "id": 30,
          },
          "prevValue": null,
          "table": "users",
        },
      ]
    `);
  });
});
