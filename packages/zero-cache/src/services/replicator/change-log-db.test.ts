import {existsSync, mkdirSync, rmSync, statSync, writeFileSync} from 'node:fs';
import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {
  createSilentLogContext,
  TestLogSink,
} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {DbFile, expectTableExact} from '../../test/lite.ts';
import {CREATE_V14_CHANGE_LOG_STREAM} from '../change-source/common/replica-schema.ts';
import {
  CHANGE_LOG_BACKFILLING_TABLE,
  ChangeLogCookieWriter,
  DROP_CHANGE_LOG_COOKIE_TABLES,
  EMPTY_COOKIE_SET,
  readCookies,
  type CookieSet,
} from './change-log-cookies.ts';
import {
  applyChangeLogPragmas,
  CHANGE_LOG_DB_SCHEMA_VERSION,
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  CHANGE_LOG_STREAM_WRITE_TIME_INDEX,
  changeLogFileName,
  CREATE_CHANGE_LOG_STREAM_SCHEMA,
  deleteChangeLogDB,
  estimateChangeLogStreamRowBytes,
  openChangeLogDB,
  openChangeLogDBForWriting,
  readChangeLogHead,
  readChangeLogMeta,
  reconcileChangeLog,
  type ChangeLogAnchor,
} from './change-log-db.ts';

const lc = createSilentLogContext();

const NOW_MS = 12345;

/**
 * The log anchors on the watermark its stream connection resumes from, plus the
 * replica's identity. Nothing here reads a replica: after the writer moved to
 * the change-streamer, the replica's state version is a consequence of the log
 * rather than a constraint on it.
 */
const ANCHOR: ChangeLogAnchor = {
  identity: {epoch: null, generation: '01', replicaID: '1777575698286'},
  resumeWatermark: '05',
  nowMs: NOW_MS,
  cookies: EMPTY_COOKIE_SET,
};

/** The main file and the sidecars `deleteLiteDB` removes. */
const SIDECARS = ['', '-wal', '-wal2', '-shm'];

const dbFiles: DbFile[] = [];

afterEach(() => {
  let file: DbFile | undefined;
  while ((file = dbFiles.pop())) {
    deleteChangeLogDB(file.path);
    file.delete();
  }
});

function newDbFile(): DbFile {
  const file = new DbFile('change-log-db-test');
  dbFiles.push(file);
  return file;
}

function anchorAt(
  resumeWatermark: string,
  overrides: Partial<ChangeLogAnchor> = {},
): ChangeLogAnchor {
  return {...ANCHOR, resumeWatermark, ...overrides};
}

/**
 * Appends a transaction the way the writer does: every row carries the
 * transaction's *commit* watermark, and only the commit row carries
 * `precommit` and `writeTimeMs`.
 */
function appendTransaction(
  db: Database,
  watermark: string,
  precommit: string,
  changes: number,
  writeTimeMs: number,
) {
  const stmt = db.prepare(/*sql*/ `
    INSERT INTO "${CHANGE_LOG_STREAM_TABLE}"
      ("watermark", "pos", "tag", "estimatedBytes", "change", "precommit",
       "writeTimeMs")
      VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  const insert = (
    pos: number,
    tag: string,
    change: string,
    rowPrecommit: string | null,
    rowWriteTimeMs: number | null,
  ) =>
    stmt.run(
      watermark,
      pos,
      tag,
      estimateChangeLogStreamRowBytes(
        watermark,
        tag,
        change,
        rowPrecommit ?? undefined,
        rowWriteTimeMs !== null,
      ),
      change,
      rowPrecommit,
      rowWriteTimeMs,
    );
  insert(0, 'begin', '{"tag":"begin"}', null, null);
  for (let i = 1; i <= changes; i++) {
    insert(i, 'insert', `{"tag":"insert","pos":${i}}`, null, null);
  }
  insert(changes + 1, 'commit', '{"tag":"commit"}', precommit, writeTimeMs);
}

function seedRows(anchor: ChangeLogAnchor) {
  return [
    {
      watermark: anchor.resumeWatermark,
      pos: 0,
      tag: 'begin',
      estimatedBytes: estimateChangeLogStreamRowBytes(
        anchor.resumeWatermark,
        'begin',
        '{"tag":"begin"}',
      ),
      change: '{"tag":"begin"}',
      precommit: null,
      writeTimeMs: null,
    },
    {
      watermark: anchor.resumeWatermark,
      pos: 1,
      tag: 'commit',
      estimatedBytes: estimateChangeLogStreamRowBytes(
        anchor.resumeWatermark,
        'commit',
        '{"tag":"commit"}',
        anchor.resumeWatermark,
        true,
      ),
      change: '{"tag":"commit"}',
      precommit: anchor.resumeWatermark,
      writeTimeMs: anchor.nowMs,
    },
  ];
}

function expectSeededAt(db: Database, anchor: ChangeLogAnchor) {
  expect(readChangeLogMeta(db)).toEqual({
    ...anchor.identity,
    schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
    seededAtMs: anchor.nowMs,
    seedWatermark: anchor.resumeWatermark,
  });
  expectTableExact(
    db,
    CHANGE_LOG_STREAM_TABLE,
    seedRows(anchor),
    'number',
    'watermark',
    'pos',
  );
}

const readPragma = Database.prototype.pragma;

/** Reads the mode through the unmocked pragma, so the degrade-path test can
 * stub `Database.prototype.pragma` without blinding its own assertions. */
function autoVacuum(db: Database): number {
  const [{auto_vacuum: mode}] = readPragma.call(db, 'auto_vacuum') as [
    {auto_vacuum: number},
  ];
  return mode;
}

/**
 * A schema v1 log: the shape 7D shipped, before the v2 meta row and before
 * `auto_vacuum`. This is what a staging file looks like on the first start
 * after slice 8.
 */
function createV1Log(replicaFile: string): void {
  using db = openChangeLogDB(lc, replicaFile, {readonly: false});
  db.pragma('journal_mode = wal2');
  db.exec(/*sql*/ `
    ${CREATE_CHANGE_LOG_STREAM_SCHEMA}
    CREATE TABLE "${CHANGE_LOG_META_TABLE}" (
      "replicaVersion" TEXT NOT NULL,
      "schemaVersion"  INTEGER NOT NULL,
      "lock"           INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
    );
    INSERT INTO "${CHANGE_LOG_META_TABLE}" ("replicaVersion", "schemaVersion")
      VALUES ('01', 1);
  `);
  appendTransaction(db, ANCHOR.resumeWatermark, '04', 1, 100);
}

/** A change-log db that is valid and consistent with {@link ANCHOR}. */
function createReconciledLog(anchor = ANCHOR): Database {
  const db = new Database(lc, ':memory:');
  reconcileChangeLog(lc, db, anchor);
  return db;
}

describe('replicator/change-log-db', () => {
  describe('schema', () => {
    test('creates the stream table and partial retention index', () => {
      using db = new Database(lc, ':memory:');
      db.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);

      expect(
        db.prepare(`PRAGMA table_info("${CHANGE_LOG_STREAM_TABLE}")`).all(),
      ).toEqual([
        {
          cid: 0,
          name: 'watermark',
          type: 'TEXT',
          notnull: 1,
          dflt_value: null,
          pk: 1,
        },
        {
          cid: 1,
          name: 'pos',
          type: 'INTEGER',
          notnull: 1,
          dflt_value: null,
          pk: 2,
        },
        {
          cid: 2,
          name: 'tag',
          type: 'TEXT',
          notnull: 1,
          dflt_value: null,
          pk: 0,
        },
        {
          cid: 3,
          name: 'estimatedBytes',
          type: 'INTEGER',
          notnull: 1,
          dflt_value: null,
          pk: 0,
        },
        {
          cid: 4,
          name: 'change',
          type: 'TEXT',
          notnull: 1,
          dflt_value: null,
          pk: 0,
        },
        {
          cid: 5,
          name: 'precommit',
          type: 'TEXT',
          notnull: 0,
          dflt_value: null,
          pk: 0,
        },
        {
          cid: 6,
          name: 'writeTimeMs',
          type: 'INTEGER',
          notnull: 0,
          dflt_value: null,
          pk: 0,
        },
      ]);

      expect(
        db.prepare(`PRAGMA index_list("${CHANGE_LOG_STREAM_TABLE}")`).all(),
      ).toEqual(
        expect.arrayContaining([
          {
            seq: expect.any(Number),
            name: CHANGE_LOG_STREAM_WRITE_TIME_INDEX,
            unique: 0,
            origin: 'c',
            partial: 1,
          },
        ]),
      );
      expect(
        db
          .prepare(`PRAGMA index_info("${CHANGE_LOG_STREAM_WRITE_TIME_INDEX}")`)
          .all(),
      ).toEqual([
        {seqno: 0, cid: 6, name: 'writeTimeMs'},
        {seqno: 1, cid: 0, name: 'watermark'},
      ]);
    });

    // The replica's migration is history and must not acquire columns added to
    // the separate, disposable change-log database after the split.
    test('keeps the replica migration 14 shape frozen apart', () => {
      using changeLog = new Database(lc, ':memory:');
      changeLog.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
      using replica = new Database(lc, ':memory:');
      replica.exec(CREATE_V14_CHANGE_LOG_STREAM);

      const columns = (db: Database) =>
        db
          .prepare(`PRAGMA table_info("${CHANGE_LOG_STREAM_TABLE}")`)
          .all<{name: string}>()
          .map(({name}) => name);
      expect(columns(replica)).toEqual([
        'watermark',
        'pos',
        'change',
        'precommit',
        'writeTimeMs',
      ]);
      expect(columns(changeLog)).toEqual([
        'watermark',
        'pos',
        'tag',
        'estimatedBytes',
        'change',
        'precommit',
        'writeTimeMs',
      ]);
    });

    // The whole v2 shape lands in one bump, so that slice 8 can add the
    // `auto_vacuum` pragma without forcing a second reseed on every task.
    test('the meta row carries the identity triple and the seed point', () => {
      using db = createReconciledLog();

      expect(readChangeLogMeta(db)).toEqual({
        epoch: null,
        generation: '01',
        replicaID: '1777575698286',
        schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
        seededAtMs: NOW_MS,
        seedWatermark: '05',
      });
    });
  });

  describe('file naming', () => {
    test('the log and its sidecars never collide with the replica', () => {
      const replica = '/data/sync-replica.db';
      expect(changeLogFileName(replica)).toBe(
        '/data/sync-replica.db-change-log',
      );

      // Every change-log sidecar is a suffix of the change-log name itself,
      // and none of them is a sidecar of the replica.
      const replicaFiles = new Set(SIDECARS.map(s => replica + s));
      const logFiles = SIDECARS.map(s => changeLogFileName(replica) + s);
      expect(logFiles.filter(f => replicaFiles.has(f))).toEqual([]);
    });

    test('deleteChangeLogDB removes only the change-log files', () => {
      const file = newDbFile();
      const replicaFiles = SIDECARS.map(s => file.path + s);
      const logFiles = SIDECARS.map(s => changeLogFileName(file.path) + s);
      for (const f of [...replicaFiles, ...logFiles]) {
        writeFileSync(f, '');
      }

      deleteChangeLogDB(file.path);

      expect(replicaFiles.filter(f => !existsSync(f))).toEqual([]);
      expect(logFiles.filter(f => existsSync(f))).toEqual([]);
    });

    test('deleteChangeLogDB is a no-op when the log is absent', () => {
      const file = newDbFile();
      expect(() => deleteChangeLogDB(file.path)).not.toThrow();
      expect(existsSync(changeLogFileName(file.path))).toBe(false);
    });
  });

  describe('openChangeLogDB', () => {
    test('creates the file beside the replica when writable', () => {
      const file = newDbFile();
      using db = openChangeLogDB(lc, file.path, {readonly: false});

      expect(db.name).toBe(changeLogFileName(file.path));
      expect(existsSync(changeLogFileName(file.path))).toBe(true);
    });

    test('opens an existing file readonly', () => {
      const file = newDbFile();
      {
        using db = openChangeLogDB(lc, file.path, {readonly: false});
        reconcileChangeLog(lc, db, ANCHOR);
      }

      using db = openChangeLogDB(lc, file.path, {readonly: true});
      expect(readChangeLogHead(db)).toBe(ANCHOR.resumeWatermark);
    });

    // Slice 7E turns this into a decline-and-fall-back-to-PG, rather than a
    // failed subscription.
    test('readonly open of an absent log throws', () => {
      const file = newDbFile();
      expect(() => openChangeLogDB(lc, file.path, {readonly: true})).toThrow();
    });
  });

  describe('applyChangeLogPragmas', () => {
    test('configures the durability the benchmark settled on', () => {
      const file = newDbFile();
      using db = openChangeLogDB(lc, file.path, {readonly: false});

      applyChangeLogPragmas(db);

      expect(db.pragma('journal_mode')).toEqual([{journal_mode: 'wal2'}]);
      // 1 == NORMAL: the commit sits on the forward path, so it does not pay
      // for an fsync.
      expect(db.pragma('synchronous')).toEqual([{synchronous: 1}]);
      expect(db.pragma('busy_timeout')).toEqual([{timeout: 30000}]);
      // Nothing else owns this file, so checkpointing stays automatic rather
      // than being handed to litestream as it is for the backup replica.
      expect(db.pragma('wal_autocheckpoint')).toEqual([
        {wal_autocheckpoint: 1000},
      ]);
      // 2 == INCREMENTAL, so purge can hand freed pages back to the OS.
      expect(autoVacuum(db)).toBe(2);
    });

    // This is the test that pins the pragma *ordering*. SQLite honours a
    // none -> incremental transition only while the file is empty, and
    // `journal_mode = wal2` materializes page 1, so appending the pragma
    // instead of prepending it produces a log that silently never reclaims.
    test('auto_vacuum survives on the reopened file, not just the connection', () => {
      const file = newDbFile();
      {
        using db = openChangeLogDB(lc, file.path, {readonly: false});
        applyChangeLogPragmas(db);
        db.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
      }

      using reopened = openChangeLogDB(lc, file.path, {readonly: true});
      expect(autoVacuum(reopened)).toBe(2);
    });

    test('appending the pragma after journal_mode would not take', () => {
      const file = newDbFile();
      {
        using db = openChangeLogDB(lc, file.path, {readonly: false});
        db.pragma('journal_mode = wal2');
        db.pragma('auto_vacuum = INCREMENTAL');
        db.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
        // Nor does dropping and recreating the tables: the mode is a header
        // property that survives `DROP TABLE`, which is why a wrong mode is
        // answered with a new file rather than with a reseed.
        db.pragma('auto_vacuum = INCREMENTAL');
        db.exec(/*sql*/ `DROP TABLE "${CHANGE_LOG_STREAM_TABLE}"`);
        db.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
        expect(autoVacuum(db)).toBe(0);
      }

      using reopened = openChangeLogDB(lc, file.path, {readonly: true});
      expect(autoVacuum(reopened)).toBe(0);
    });
  });

  describe('openChangeLogDBForWriting', () => {
    test('creates and seeds the log, then leaves it alone', () => {
      const file = newDbFile();
      {
        const {db, result} = openChangeLogDBForWriting(lc, file.path, ANCHOR);
        using open = db;
        expect(result).toEqual({
          action: 'reseeded',
          head: ANCHOR.resumeWatermark,
          reason: 'created',
          cookiesStale: true,
        });
        expect(open.pragma('journal_mode')).toEqual([{journal_mode: 'wal2'}]);
        expect(readChangeLogHead(open)).toBe(ANCHOR.resumeWatermark);
      }

      const reopened = openChangeLogDBForWriting(lc, file.path, ANCHOR);
      using db = reopened.db;
      expect(reopened.result).toEqual({
        action: 'none',
        head: ANCHOR.resumeWatermark,
        cookiesStale: false,
      });
      expect(readChangeLogHead(db)).toBe(ANCHOR.resumeWatermark);
    });

    // The divergence from replica corruption: the log is a cache, so a corrupt
    // file costs a retention window rather than a restore or an auto-reset.
    test('rebuilds a corrupt log rather than failing startup', () => {
      const file = newDbFile();
      writeFileSync(changeLogFileName(file.path), 'this is not a database');

      const {db, result} = openChangeLogDBForWriting(lc, file.path, ANCHOR);
      using rebuilt = db;

      expect(result).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'created',
        cookiesStale: true,
      });
      expectSeededAt(rebuilt, ANCHOR);
      expect(rebuilt.pragma('journal_mode')).toEqual([{journal_mode: 'wal2'}]);
    });

    test('rebuilds a v1 log and enables incremental vacuum', () => {
      const file = newDbFile();
      createV1Log(file.path);

      const {db, result} = openChangeLogDBForWriting(lc, file.path, ANCHOR);
      using rebuilt = db;

      // The file is replaced rather than dropped in place, but the original
      // reason is retained for observability.
      expect(result).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'schema-mismatch',
        cookiesStale: true,
      });
      expectSeededAt(rebuilt, ANCHOR);
      expect(autoVacuum(rebuilt)).toBe(2);
    });

    // The guard is on the pragma, not only on the version: every log the
    // writer created before slice 8 carries the current schema version with
    // `auto_vacuum = 0`, so this is the upgrade path rather than an edge case.
    // Without it such a file would reconcile clean and never reclaim.
    test('rebuilds a current-version log whose pragma never took', () => {
      const file = newDbFile();
      {
        using db = openChangeLogDB(lc, file.path, {readonly: false});
        db.pragma('journal_mode = wal2');
        db.pragma('auto_vacuum = INCREMENTAL'); // too late; stays at 0
        reconcileChangeLog(lc, db, ANCHOR);
        appendTransaction(db, '07', '05', 2, 100);
        expect(autoVacuum(db)).toBe(0);
      }

      const {db, result} = openChangeLogDBForWriting(lc, file.path, ANCHOR);
      using rebuilt = db;

      expect(result).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'created',
        cookiesStale: true,
      });
      expect(autoVacuum(rebuilt)).toBe(2);
    });

    // A pragma that is still wrong on a brand-new file can only mean the pragma
    // sequence itself is wrong, which is a code bug present on every start.
    // Throwing would be a crash loop and rebuilding again would be a reseed per
    // start, so the log opens and `Database.compact()`'s own mode guard becomes
    // the fallback.
    test('opens a log whose pragma is still wrong after one rebuild', () => {
      const file = newDbFile();
      const sink = new TestLogSink();
      const logger = new LogContext('error', undefined, sink);
      const reads = vi
        .spyOn(Database.prototype, 'pragma')
        .mockImplementation(function (this: Database, sql: string) {
          return sql === 'auto_vacuum'
            ? [{auto_vacuum: 0}]
            : // oxlint-disable-next-line no-explicit-any
              (readPragma.call(this, sql) as any);
        });

      try {
        const {db, result} = openChangeLogDBForWriting(
          logger,
          file.path,
          ANCHOR,
        );
        using opened = db;

        expect(result).toEqual({
          action: 'reseeded',
          head: ANCHOR.resumeWatermark,
          reason: 'created',
          cookiesStale: true,
        });
        expect(readChangeLogHead(opened)).toBe(ANCHOR.resumeWatermark);
        // One read for the initial open and one for the rebuild: no third.
        expect(
          reads.mock.calls.filter(([sql]) => sql === 'auto_vacuum'),
        ).toHaveLength(2);
      } finally {
        reads.mockRestore();
      }

      expect(sink.messages.filter(([level]) => level === 'error')).toHaveLength(
        1,
      );
      expect(sink.messages.at(-1)?.[2][0]).toMatch(
        /does not support incremental vacuum/,
      );
    });

    // A path that cannot be opened is a problem with the environment rather
    // than with the file's contents, so it propagates with the file intact.
    // The caller answers it by disabling the writer and continuing to
    // replicate, not by deleting a database that is not the problem.
    test('does not rebuild for a failure that is not corruption', () => {
      const file = newDbFile();
      const dir = changeLogFileName(file.path);
      mkdirSync(dir);
      try {
        expect(() => openChangeLogDBForWriting(lc, file.path, ANCHOR)).toThrow(
          /unable to open database file/,
        );
        expect(statSync(dir).isDirectory()).toBe(true);
      } finally {
        rmSync(dir, {recursive: true, force: true});
      }
    });
  });

  describe('reconcileChangeLog: reseed reasons', () => {
    test('created: empty database', () => {
      using db = new Database(lc, ':memory:');

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'created',
        cookiesStale: true,
      });
      expectSeededAt(db, ANCHOR);
    });

    test('created: stream table without the meta table', () => {
      using db = new Database(lc, ':memory:');
      db.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
      appendTransaction(db, '05', '04', 3, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'created',
        cookiesStale: true,
      });
      // The pre-existing rows are wiped, not retained.
      expectSeededAt(db, ANCHOR);
    });

    test('created: meta table without its row', () => {
      using db = createReconciledLog();
      db.exec(/*sql*/ `DELETE FROM "${CHANGE_LOG_META_TABLE}"`);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'created',
        cookiesStale: true,
      });
      expectSeededAt(db, ANCHOR);
    });

    test('created: meta table without the stream table', () => {
      using db = createReconciledLog();
      db.exec(/*sql*/ `DROP TABLE "${CHANGE_LOG_STREAM_TABLE}"`);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'created',
        cookiesStale: true,
      });
      expect(readChangeLogHead(db)).toBe(ANCHOR.resumeWatermark);
    });

    test('schema-mismatch', () => {
      using db = createReconciledLog();
      appendTransaction(db, '07', '05', 3, 100);
      db.exec(/*sql*/ `
        UPDATE "${CHANGE_LOG_META_TABLE}"
          SET "schemaVersion" = ${CHANGE_LOG_DB_SCHEMA_VERSION + 1}
      `);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'schema-mismatch',
        cookiesStale: true,
      });
      expectSeededAt(db, ANCHOR);
    });

    // A v1 log carries a "replicaVersion" where v2 carries the identity triple,
    // so the version is read on its own: reading the rest of the row would fail
    // on the missing columns instead of reporting the mismatch that explains it.
    // This is the upgrade path every existing log takes.
    test('schema-mismatch: a v1 log is rebuilt rather than read', () => {
      using db = new Database(lc, ':memory:');
      db.exec(/*sql*/ `
        ${CREATE_CHANGE_LOG_STREAM_SCHEMA}
        CREATE TABLE "${CHANGE_LOG_META_TABLE}" (
          "replicaVersion" TEXT NOT NULL,
          "schemaVersion"  INTEGER NOT NULL,
          "lock"           INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
        );
        INSERT INTO "${CHANGE_LOG_META_TABLE}" ("replicaVersion", "schemaVersion")
          VALUES ('01', 1);
      `);
      appendTransaction(db, '05', '04', 1, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'schema-mismatch',
        cookiesStale: true,
      });
      expectSeededAt(db, ANCHOR);
    });

    test.each([
      [
        'a different generation',
        {epoch: null, generation: '00', replicaID: 'a'},
      ],
      ['a sibling replica ID', {epoch: null, generation: '01', replicaID: 'b'}],
      ['a different epoch', {epoch: '1', generation: '01', replicaID: 'a'}],
    ])('identity-mismatch: %s', (_, identity) => {
      // A leftover log on a reused volume. Under RMv2 the generation alone
      // cannot catch this: siblings of a forked replica share it.
      using db = createReconciledLog(anchorAt('05', {identity}));
      appendTransaction(db, '07', '05', 3, 100);

      const anchor = anchorAt('05', {
        identity: {epoch: null, generation: '01', replicaID: 'a'},
      });
      expect(reconcileChangeLog(lc, db, anchor)).toEqual({
        action: 'reseeded',
        head: '05',
        reason: 'identity-mismatch',
        cookiesStale: true,
      });
      expectSeededAt(db, anchor);
    });

    test('identity: a null epoch and replica ID match only another null', () => {
      const identity = {epoch: null, generation: '01', replicaID: null};
      using db = createReconciledLog(anchorAt('05', {identity}));

      expect(reconcileChangeLog(lc, db, anchorAt('05', {identity}))).toEqual({
        action: 'none',
        head: '05',
        cookiesStale: false,
      });
      expect(
        reconcileChangeLog(
          lc,
          db,
          anchorAt('05', {identity: {...identity, replicaID: 'a'}}),
        ),
      ).toMatchObject({action: 'reseeded', reason: 'identity-mismatch'});
    });

    test('gap: valid meta with an empty log', () => {
      using db = createReconciledLog();
      db.exec(/*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"`);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'gap',
        cookiesStale: true,
      });
      expect(readChangeLogHead(db)).toBe(ANCHOR.resumeWatermark);
    });

    // The log leads the resume watermark by construction, so this is the state
    // that says the ordering guarantee did not hold: the writer was disabled for
    // a while, the file was deleted, or power was lost with synchronous=NORMAL.
    test('gap: head behind the resume watermark', () => {
      using db = createReconciledLog(anchorAt('03'));
      appendTransaction(db, '04', '03', 2, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'gap',
        cookiesStale: true,
      });
      expectSeededAt(db, ANCHOR);
    });

    test('gap: truncation leaves the head behind the resume watermark', () => {
      using db = createReconciledLog(anchorAt('03'));
      appendTransaction(db, '07', '03', 2, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'gap',
        cookiesStale: true,
      });
      expect(readChangeLogHead(db)).toBe(ANCHOR.resumeWatermark);
    });
  });

  describe('reconcileChangeLog: truncation', () => {
    test('removes rows above the resume watermark and retains the rest', () => {
      using db = createReconciledLog(anchorAt('02'));
      appendTransaction(db, '03', '02', 1, 100);
      appendTransaction(db, '05', '03', 1, 200);
      appendTransaction(db, '06', '05', 1, 300);
      appendTransaction(db, '07', '06', 2, 400);

      // 3 rows in '06' + 4 rows in '07'
      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'truncated',
        head: ANCHOR.resumeWatermark,
        rows: 7,
        cookiesStale: true,
      });
      expect(readChangeLogHead(db)).toBe(ANCHOR.resumeWatermark);
      expect(
        db
          .prepare(/*sql*/ `SELECT DISTINCT "watermark" FROM "${CHANGE_LOG_STREAM_TABLE}"
                       ORDER BY "watermark"`)
          .all(),
      ).toEqual([{watermark: '02'}, {watermark: '03'}, {watermark: '05'}]);
      // The log's new head transaction is untouched by the truncation, so it is
      // still a valid catchup boundary for a subscriber sitting exactly there.
      expect(
        db
          .prepare(/*sql*/ `SELECT * FROM "${CHANGE_LOG_STREAM_TABLE}"
                       WHERE "watermark" = '05' ORDER BY "pos"`)
          .all(),
      ).toEqual([
        {
          watermark: '05',
          pos: 0,
          tag: 'begin',
          estimatedBytes: estimateChangeLogStreamRowBytes(
            '05',
            'begin',
            '{"tag":"begin"}',
          ),
          change: '{"tag":"begin"}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '05',
          pos: 1,
          tag: 'insert',
          estimatedBytes: estimateChangeLogStreamRowBytes(
            '05',
            'insert',
            '{"tag":"insert","pos":1}',
          ),
          change: '{"tag":"insert","pos":1}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '05',
          pos: 2,
          tag: 'commit',
          estimatedBytes: estimateChangeLogStreamRowBytes(
            '05',
            'commit',
            '{"tag":"commit"}',
            '03',
            true,
          ),
          change: '{"tag":"commit"}',
          precommit: '03',
          writeTimeMs: 200,
        },
      ]);
    });

    test('removes a re-delivered transaction whole', () => {
      // Every row of a transaction carries its commit watermark, so truncation
      // cannot leave an orphaned begin or a mid-transaction row behind.
      using db = createReconciledLog();
      appendTransaction(db, '09', '05', 25, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'truncated',
        head: ANCHOR.resumeWatermark,
        rows: 27,
        cookiesStale: true,
      });
      expect(
        db
          .prepare(/*sql*/ `SELECT count(*) AS "rows" FROM "${CHANGE_LOG_STREAM_TABLE}"
                       WHERE "watermark" > @resumeWatermark`)
          .get<{rows: number}>({resumeWatermark: ANCHOR.resumeWatermark}).rows,
      ).toBe(0);
      expectTableExact(
        db,
        CHANGE_LOG_STREAM_TABLE,
        seedRows(ANCHOR),
        'number',
        'watermark',
        'pos',
      );
    });

    test('is idempotent', () => {
      using db = createReconciledLog();
      appendTransaction(db, '09', '05', 2, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toMatchObject({
        action: 'truncated',
      });
      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'none',
        head: ANCHOR.resumeWatermark,
        cookiesStale: false,
      });
      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'none',
        head: ANCHOR.resumeWatermark,
        cookiesStale: false,
      });
    });

    // `seededAtMs` is how a canary decides a log is warm enough to serve, so it
    // must move on every reseed and stay put through everything else.
    test('seededAtMs moves on a reseed and survives a truncation', () => {
      using db = createReconciledLog();
      expect(readChangeLogMeta(db)).toMatchObject({
        seededAtMs: NOW_MS,
        seedWatermark: '05',
      });

      const later = anchorAt('05', {nowMs: NOW_MS + 60_000});
      appendTransaction(db, '09', '05', 2, 100);
      expect(reconcileChangeLog(lc, db, later)).toEqual({
        action: 'truncated',
        head: '05',
        rows: 4,
        cookiesStale: true,
      });
      // The log did not start over, so its warm-up clock does not restart.
      expect(readChangeLogMeta(db)).toMatchObject({seededAtMs: NOW_MS});

      const reseeded = anchorAt('07', {nowMs: NOW_MS + 60_000});
      expect(reconcileChangeLog(lc, db, reseeded)).toEqual({
        action: 'reseeded',
        head: '07',
        reason: 'gap',
        cookiesStale: true,
      });
      expect(readChangeLogMeta(db)).toMatchObject({
        seededAtMs: NOW_MS + 60_000,
        seedWatermark: '07',
      });
    });

    // The reconnect case, which is what makes this per-connection rather than
    // per-process: the log holds transactions the resumed stream will
    // re-deliver, and the writer's plain INSERT would collide with them.
    test('a resume below the head truncates rather than colliding', () => {
      using db = createReconciledLog(anchorAt('02'));
      appendTransaction(db, '03', '02', 1, 100);
      appendTransaction(db, '05', '03', 1, 200);

      expect(reconcileChangeLog(lc, db, anchorAt('03'))).toEqual({
        action: 'truncated',
        head: '03',
        rows: 3,
        cookiesStale: true,
      });
      // Re-appending what was truncated no longer violates the primary key.
      expect(() => appendTransaction(db, '05', '03', 1, 300)).not.toThrow();
    });
  });

  describe('reconcileChangeLog: consistent log', () => {
    test('a freshly seeded log at the resume watermark is a no-op', () => {
      using db = createReconciledLog();

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'none',
        head: ANCHOR.resumeWatermark,
        cookiesStale: false,
      });
      expectSeededAt(db, ANCHOR);
    });

    test('a log at the resume watermark is not written to', () => {
      const file = newDbFile();
      using db = openChangeLogDB(lc, file.path, {readonly: false});
      reconcileChangeLog(lc, db, ANCHOR);
      appendTransaction(db, '07', '05', 2, 100);

      // data_version only advances for writes made by *another* connection.
      using observer = openChangeLogDB(lc, file.path, {readonly: true});
      const dataVersion = () => observer.pragma('data_version');
      const before = dataVersion();

      expect(reconcileChangeLog(lc, db, anchorAt('07'))).toEqual({
        action: 'none',
        head: '07',
        cookiesStale: false,
      });
      expect(dataVersion()).toEqual(before);

      // Control: a write does move it.
      appendTransaction(db, '09', '07', 1, 200);
      expect(dataVersion()).not.toEqual(before);
    });
  });

  describe('reconcileChangeLog: transaction', () => {
    test('rolls back everything on failure', () => {
      using db = createReconciledLog();
      appendTransaction(db, '09', '05', 2, 100);
      db.exec(/*sql*/ `
        CREATE TRIGGER "no_deletes" BEFORE DELETE ON "${CHANGE_LOG_STREAM_TABLE}"
        BEGIN SELECT RAISE(ABORT, 'boom'); END;
      `);

      expect(() => reconcileChangeLog(lc, db, ANCHOR)).toThrow('boom');
      expect(db.inTransaction).toBe(false);
      // The re-delivered transaction is still there: nothing was half-applied.
      expect(readChangeLogHead(db)).toBe('09');
    });

    test('leaves no transaction open when a reseed fails', () => {
      const file = newDbFile();
      {
        using db = openChangeLogDB(lc, file.path, {readonly: false});
        reconcileChangeLog(
          lc,
          db,
          anchorAt('05', {
            identity: {epoch: null, generation: '00', replicaID: null},
          }),
        );
      }

      using db = openChangeLogDB(lc, file.path, {readonly: true});
      expect(() => reconcileChangeLog(lc, db, ANCHOR)).toThrow(
        'attempt to write a readonly database',
      );
      expect(db.inTransaction).toBe(false);
    });

    test('does not roll back a transaction the failure already closed', () => {
      // SQLite rolls back automatically for some errors (SQLITE_FULL,
      // SQLITE_IOERR); issuing ROLLBACK then throws and masks the real cause.
      const cause = new Error('database or disk is full');
      const executed: string[] = [];
      const db = {
        inTransaction: false,
        exec: (sql: string) => void executed.push(sql),
        prepare: () => {
          throw cause;
        },
      } as unknown as Database;

      expect(() => reconcileChangeLog(lc, db, ANCHOR)).toThrow(cause);
      expect(executed).toEqual(['BEGIN IMMEDIATE']);
    });
  });

  // The cookies are unversioned point-in-time state: they are only meaningful
  // paired with the watermark they were folded to. So they are created with the
  // buffer, dropped with it, and — whenever reconciliation moves the head —
  // replaced with the set the anchor carries for the resume watermark.
  describe('the cookie jar', () => {
    /** Puts a cookie in the jar without going through the writer. */
    function seedCookie(db: Database) {
      new ChangeLogCookieWriter(db).apply({
        tag: 'create-table',
        spec: {schema: 'my', name: 'foo', columns: {}},
        metadata: {rowKey: {columns: ['id']}},
        backfill: {a: {fooID: 1}},
      });
    }

    /**
     * The cookie set Postgres holds at the resume watermark, in the shape
     * `Storer.getStartStreamInitializationParameters()` returns it. It is
     * deliberately disjoint from what {@link seedCookie} folds, so that
     * "installed the anchor's" and "kept the log's own" cannot both pass.
     */
    const ANCHOR_COOKIES: CookieSet = {
      tableMetadata: [
        {schema: 'my', table: 'bar', metadata: {rowKey: {columns: ['barID']}}},
      ],
      backfilling: [
        {schema: 'my', table: 'bar', column: 'z', backfill: {barID: 9}},
      ],
    };

    test('a reseed creates the cookie tables, empty', () => {
      using db = createReconciledLog();

      expect(readCookies(db)).toEqual(EMPTY_COOKIE_SET);
    });

    test('a reseed installs the cookie set the anchor carries', () => {
      using db = createReconciledLog(anchorAt('05', {cookies: ANCHOR_COOKIES}));

      expect(readCookies(db)).toEqual(ANCHOR_COOKIES);
    });

    test('a wipe drops the log’s cookies and installs the anchor’s', () => {
      using db = createReconciledLog();
      seedCookie(db);
      appendTransaction(db, '09', '05', 1, 100);

      // An identity mismatch: this file belongs to a different replica, so its
      // cookie set describes a different stream and none of it survives.
      const anchor = anchorAt('05', {
        identity: {epoch: null, generation: '02', replicaID: 'other'},
        cookies: ANCHOR_COOKIES,
      });
      expect(reconcileChangeLog(lc, db, anchor)).toMatchObject({
        action: 'reseeded',
        reason: 'identity-mismatch',
        cookiesStale: true,
      });
      expect(readCookies(db)).toEqual(ANCHOR_COOKIES);
    });

    // Invariant 17. The truncated transactions could have carried a
    // `create-table`, an `add-column`, or a `backfill-completed`, none of which
    // deleting their rows rolls back — so the set is replaced rather than
    // rewound, and the replacement is the one that belongs to the resume point.
    test('a truncation replaces the cookies it can no longer vouch for', () => {
      using db = createReconciledLog();
      seedCookie(db);
      appendTransaction(db, '09', '05', 2, 100);

      expect(
        reconcileChangeLog(lc, db, anchorAt('05', {cookies: ANCHOR_COOKIES})),
      ).toMatchObject({action: 'truncated', cookiesStale: true});
      expect(readCookies(db)).toEqual(ANCHOR_COOKIES);
    });

    test('a truncation with an empty anchor set clears the jar', () => {
      using db = createReconciledLog();
      seedCookie(db);
      appendTransaction(db, '09', '05', 2, 100);

      // The normal case: no backfill is in flight at the resume watermark, so
      // the set that belongs there is empty. It is still installed rather than
      // assumed — an emptied jar and an untouched one are not the same thing.
      expect(reconcileChangeLog(lc, db, ANCHOR)).toMatchObject({
        action: 'truncated',
        cookiesStale: true,
      });
      expect(readCookies(db)).toEqual(EMPTY_COOKIE_SET);
    });

    test('a reconcile that changes nothing leaves the cookies alone', () => {
      using db = createReconciledLog();
      seedCookie(db);
      const before = readCookies(db);

      // The head is already the resume watermark, so the log's own set is the
      // one that belongs to it and the anchor's is not written over it.
      expect(
        reconcileChangeLog(lc, db, anchorAt('05', {cookies: ANCHOR_COOKIES})),
      ).toEqual({
        action: 'none',
        head: ANCHOR.resumeWatermark,
        cookiesStale: false,
      });
      expect(readCookies(db)).toEqual(before);
      expect(before.backfilling).toHaveLength(1);
    });

    // Every file in the fleet at the v3 upgrade is a v2 file, and the reason it
    // is wiped should be the version it is at rather than the tables it lacks.
    test('a v2 file reseeds as schema-mismatch, not as created', () => {
      using db = createReconciledLog();
      db.exec(/*sql*/ `
        ${DROP_CHANGE_LOG_COOKIE_TABLES}
        UPDATE "${CHANGE_LOG_META_TABLE}" SET "schemaVersion" = 2
      `);

      const anchor = anchorAt('05', {cookies: ANCHOR_COOKIES});
      expect(reconcileChangeLog(lc, db, anchor)).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'schema-mismatch',
        cookiesStale: true,
      });
      expectSeededAt(db, anchor);
      // The tables the upgrade creates are not merely present: they hold the
      // set the resumed stream is about to fold onto.
      expect(readCookies(db)).toEqual(ANCHOR_COOKIES);
    });

    test('a current-version file missing a cookie table is rebuilt', () => {
      using db = createReconciledLog();
      db.exec(/*sql*/ `DROP TABLE "${CHANGE_LOG_BACKFILLING_TABLE}"`);

      expect(
        reconcileChangeLog(lc, db, anchorAt('05', {cookies: ANCHOR_COOKIES})),
      ).toEqual({
        action: 'reseeded',
        head: ANCHOR.resumeWatermark,
        reason: 'created',
        cookiesStale: true,
      });
      expect(readCookies(db)).toEqual(ANCHOR_COOKIES);
    });

    // The whole reconciliation is one transaction, so a failure cannot leave
    // the log's head at one position and its cookie set at another.
    test('a failed reconcile rolls the cookie replacement back with it', () => {
      using db = createReconciledLog();
      seedCookie(db);
      const before = readCookies(db);
      appendTransaction(db, '09', '05', 2, 100);
      db.exec(/*sql*/ `
        CREATE TRIGGER "no_deletes" BEFORE DELETE ON "${CHANGE_LOG_STREAM_TABLE}"
        BEGIN SELECT RAISE(ABORT, 'boom'); END;
      `);

      expect(() =>
        reconcileChangeLog(lc, db, anchorAt('05', {cookies: ANCHOR_COOKIES})),
      ).toThrow('boom');
      expect(readChangeLogHead(db)).toBe('09');
      expect(readCookies(db)).toEqual(before);
    });
  });
});
