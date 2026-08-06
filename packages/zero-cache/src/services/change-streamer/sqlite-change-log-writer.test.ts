import {existsSync} from 'node:fs';
import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import {DbFile} from '../../test/lite.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {readCookies, type CookieSet} from '../replicator/change-log-cookies.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  changeLogFileName,
  deleteChangeLogDB,
  openChangeLogDB,
  readChangeLogHead,
  type ChangeLogIdentity,
} from '../replicator/change-log-db.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {serializeChangeStreamData} from './change-log-codec.ts';
import {SQLiteChangeLogWriter} from './sqlite-change-log-writer.ts';

const messages = new ReplicationMessages({foo: 'id'});

const IDENTITY: ChangeLogIdentity = {
  epoch: null,
  generation: '01',
  replicaID: 'replica-id',
};

const files: DbFile[] = [];

afterEach(() => {
  let file: DbFile | undefined;
  while ((file = files.pop())) {
    deleteChangeLogDB(file.path);
    file.delete();
  }
});

function setup(name: string, resumeWatermark = '02') {
  const sink = new TestLogSink();
  const lc = new LogContext('debug', undefined, sink);
  const file = new DbFile(name);
  files.push(file);
  const commits: string[] = [];
  let disabled = 0;
  const writer = new SQLiteChangeLogWriter(lc, {
    replicaFile: file.path,
    identity: IDENTITY,
    onCommit: watermark => commits.push(watermark),
    onDisabled: () => disabled++,
    now: () => 1_700_000_000_000,
  });
  writer.reconcile(resumeWatermark);
  return {
    lc,
    sink,
    file,
    writer,
    commits,
    disabledCount: () => disabled,
    head: () => {
      using db = openChangeLogDB(lc, file.path, {readonly: true});
      return readChangeLogHead(db);
    },
    /** Read through a second connection, i.e. what is durable. */
    cookies: (): CookieSet => {
      using db = openChangeLogDB(lc, file.path, {readonly: true});
      return readCookies(db);
    },
    errors: () =>
      sink.messages
        .filter(([level]) => level === 'error')
        .map(([, , args]) => String(args[0])),
    [Symbol.dispose]() {
      writer.close();
    },
  };
}

/** Drives a whole transaction through the writer, as the stream loop does. */
function transaction(
  writer: SQLiteChangeLogWriter,
  watermark: string,
  ...data: ChangeStreamData[]
) {
  const write = (change: ChangeStreamData) =>
    writer.write(change, serializeChangeStreamData(change));
  write(['begin', messages.begin(), {commitWatermark: watermark}]);
  data.forEach(write);
  write(['commit', messages.commit(), {watermark}]);
}

describe('change-streamer/sqlite-change-log-writer', () => {
  test('appends transactions and reports each new head', () => {
    using fixture = setup('change-log-writer-append');

    transaction(fixture.writer, '04', [
      'data',
      messages.insert('foo', {id: 'one'}),
    ]);
    transaction(fixture.writer, '06', [
      'data',
      messages.insert('foo', {id: 'two'}),
    ]);

    expect(fixture.commits).toEqual(['04', '06']);
    expect(fixture.head()).toBe('06');
    expect(fixture.writer.state()).toMatchObject({
      sqliteHead: '06',
      receivedHead: '06',
      headLag: 0,
      invariantFailures: 0,
      hashMatches: 2,
      hashMismatches: 0,
      hashUnpaired: 0,
    });
  });

  test('an upstream rollback leaves no rows', () => {
    using fixture = setup('change-log-writer-rollback');

    fixture.writer.write(
      ['begin', messages.begin(), {commitWatermark: '04'}],
      serializeChangeStreamData([
        'begin',
        messages.begin(),
        {commitWatermark: '04'},
      ]),
    );
    const rollback: ChangeStreamData = ['rollback', {tag: 'rollback'}];
    fixture.writer.write(rollback, serializeChangeStreamData(rollback));

    expect(fixture.head()).toBe('02');
    expect(fixture.commits).toEqual([]);
    expect(fixture.writer.enabled).toBe(true);
  });

  // §3.7: nothing in the log is not either already durable in the replica's
  // backup or re-derivable from the slot, so a write failure must cost catchup
  // reach rather than the shard's replication.
  test('a write error disables the writer, deletes the file, and keeps going', () => {
    using fixture = setup('change-log-writer-fail-soft');
    // Pulls the table out from under the writer's prepared statements, which is
    // an error the file's contents cannot explain -- the shape a full disk or an
    // I/O error takes.
    {
      using other = openChangeLogDB(fixture.lc, fixture.file.path, {
        readonly: false,
      });
      other.exec(`DROP TABLE "${CHANGE_LOG_STREAM_TABLE}"`);
    }

    transaction(fixture.writer, '04');

    expect(fixture.writer.enabled).toBe(false);
    expect(fixture.disabledCount()).toBe(1);
    expect(existsSync(changeLogFileName(fixture.file.path))).toBe(false);
    expect(fixture.errors().join('\n')).toContain(
      'error writing to the SQLite change log',
    );
    // The generic policy, not the carve-out: a dropped table is not a missed
    // truncate-above, so it must not be reported as an invariant failure.
    expect(fixture.errors().join('\n')).not.toContain('violated a constraint');

    // Replication continues: further writes are no-ops rather than throws, and
    // nothing recreates the file.
    expect(() => transaction(fixture.writer, '06')).not.toThrow();
    fixture.writer.reconcile('06');
    expect(existsSync(changeLogFileName(fixture.file.path))).toBe(false);
    expect(fixture.writer.enabled).toBe(false);
  });

  // The carve-out that keeps fail-soft from hiding the bug class 7I introduces:
  // a missed truncate-above is a constraint violation on the writer's plain
  // INSERT, which is otherwise indistinguishable from a lost cache.
  test('a constraint violation is counted as an invariant failure', () => {
    using fixture = setup('change-log-writer-constraint');
    // A transaction the log already holds, i.e. what reconciliation should have
    // truncated before the stream re-delivered it.
    {
      using other = openChangeLogDB(fixture.lc, fixture.file.path, {
        readonly: false,
      });
      other
        .prepare(/*sql*/ `
          INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
            ("watermark", "pos", "change") VALUES ('04', 0, '{"tag":"begin"}')
        `)
        .run();
    }

    transaction(fixture.writer, '04');

    expect(fixture.errors().join('\n')).toContain(
      'violated a constraint, which means a transaction above the resume ' +
        'watermark was not truncated',
    );
    // Reported before failing soft, so the invariant failure is not absorbed.
    expect(fixture.writer.enabled).toBe(false);
    expect(existsSync(changeLogFileName(fixture.file.path))).toBe(false);
  });

  test('abort discards an interrupted transaction', () => {
    using fixture = setup('change-log-writer-abort');

    const begin: ChangeStreamData = [
      'begin',
      messages.begin(),
      {commitWatermark: '04'},
    ];
    fixture.writer.write(begin, serializeChangeStreamData(begin));
    fixture.writer.abort();

    expect(fixture.head()).toBe('02');
    expect(fixture.commits).toEqual([]);
    expect(fixture.writer.enabled).toBe(true);

    // The reconnected stream reconciles and re-delivers as if the interrupted
    // transaction had never arrived.
    fixture.writer.reconcile('02');
    transaction(fixture.writer, '04');
    expect(fixture.head()).toBe('04');
    expect(fixture.commits).toEqual(['04']);
  });

  // The stream loop aborts whenever it believes a transaction is open, which
  // includes the window between the commit-row write and its own bookkeeping --
  // so an abort can arrive after the log already committed, and must be a
  // no-op rather than a fail-soft.
  test('abort with no open transaction is a no-op', () => {
    using fixture = setup('change-log-writer-abort-idle');

    transaction(fixture.writer, '04');
    fixture.writer.abort();

    expect(fixture.writer.enabled).toBe(true);
    expect(fixture.head()).toBe('04');
    expect(fixture.errors()).toEqual([]);
  });

  test('a failure to open the log disables the writer without throwing', () => {
    const sink = new TestLogSink();
    const lc = new LogContext('debug', undefined, sink);
    const writer = new SQLiteChangeLogWriter(lc, {
      // A directory that does not exist: a bad path is an environment problem,
      // not a corrupt file, so it is not rebuilt through.
      replicaFile: '/nonexistent-directory/replica.db',
      identity: IDENTITY,
    });

    expect(() => writer.reconcile('02')).not.toThrow();
    expect(writer.enabled).toBe(false);
    expect(
      sink.messages
        .filter(([level]) => level === 'error')
        .map(([, , args]) => String(args[0]))
        .join('\n'),
    ).toContain('error reconciling the SQLite change log');
  });

  test('reconciling per connection truncates and re-accepts a re-delivery', () => {
    using fixture = setup('change-log-writer-reconnect');

    transaction(fixture.writer, '04', [
      'data',
      messages.insert('foo', {id: 'one'}),
    ]);
    transaction(fixture.writer, '06', [
      'data',
      messages.insert('foo', {id: 'two'}),
    ]);
    expect(fixture.head()).toBe('06');

    // The stream reconnects and resumes below the head.
    fixture.writer.reconcile('04');
    expect(fixture.head()).toBe('04');

    transaction(fixture.writer, '06', [
      'data',
      messages.insert('foo', {id: 'two again'}),
    ]);
    expect(fixture.head()).toBe('06');
    expect(fixture.writer.enabled).toBe(true);
    expect(fixture.writer.state()?.invariantFailures).toBe(0);
  });

  describe('the cookie jar', () => {
    const createTable: ChangeStreamData = [
      'data',
      {
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        metadata: {rowKey: {columns: ['id']}},
        backfill: {a: {fooID: 1}},
      },
    ];

    test('a schema change moves the cookies and is counted', () => {
      using fixture = setup('change-log-writer-cookies');

      expect(fixture.writer.state()).toMatchObject({
        cookieMutations: 0,
        cookieRows: {tableMetadata: 0, backfilling: 0},
      });

      transaction(fixture.writer, '04', createTable);

      expect(fixture.cookies()).toEqual({
        tableMetadata: [
          {schema: 'my', table: 'foo', metadata: {rowKey: {columns: ['id']}}},
        ],
        backfilling: [
          {schema: 'my', table: 'foo', column: 'a', backfill: {fooID: 1}},
        ],
      });
      expect(fixture.writer.state()).toMatchObject({
        cookieMutations: 2,
        cookieRows: {tableMetadata: 1, backfilling: 1},
      });

      // A transaction with no schema change neither moves nor re-counts them.
      transaction(fixture.writer, '06', [
        'data',
        messages.insert('foo', {id: 'one'}),
      ]);
      expect(fixture.writer.state()).toMatchObject({cookieMutations: 2});
    });

    test('an interrupted transaction discards its cookies with its rows', () => {
      using fixture = setup('change-log-writer-cookies-abort');

      const begin: ChangeStreamData = [
        'begin',
        messages.begin(),
        {commitWatermark: '04'},
      ];
      fixture.writer.write(begin, serializeChangeStreamData(begin));
      fixture.writer.write(createTable, serializeChangeStreamData(createTable));
      fixture.writer.abort();

      expect(fixture.head()).toBe('02');
      expect(fixture.cookies()).toEqual({tableMetadata: [], backfilling: []});
      expect(fixture.writer.state()).toMatchObject({cookieMutations: 0});
      expect(fixture.writer.enabled).toBe(true);
    });

    // Invariant 17: the cookie set is only meaningful paired with the watermark
    // it was folded to, and a truncation moves that watermark.
    test('a reconcile that truncates leaves no stale cookies behind', () => {
      using fixture = setup('change-log-writer-cookies-reconcile');

      transaction(fixture.writer, '04', createTable);
      expect(fixture.writer.state()).toMatchObject({
        cookieRows: {tableMetadata: 1, backfilling: 1},
      });

      // The stream reconnects below the head, so the transaction that carried
      // the cookies is truncated away.
      fixture.writer.reconcile('02');

      expect(fixture.cookies()).toEqual({tableMetadata: [], backfilling: []});
      expect(fixture.writer.state()).toMatchObject({
        cookieRows: {tableMetadata: 0, backfilling: 0},
      });
    });
  });
});
