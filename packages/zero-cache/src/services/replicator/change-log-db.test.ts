import {existsSync, mkdirSync, rmSync, statSync, writeFileSync} from 'node:fs';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {DbFile, expectTableExact} from '../../test/lite.ts';
import {CREATE_V14_CHANGE_LOG_STREAM} from '../change-source/common/replica-schema.ts';
import {
  applyChangeLogPragmas,
  CHANGE_LOG_DB_SCHEMA_VERSION,
  CHANGE_LOG_META_TABLE,
  CHANGE_LOG_STREAM_TABLE,
  CHANGE_LOG_STREAM_WRITE_TIME_INDEX,
  changeLogFileName,
  CREATE_CHANGE_LOG_STREAM_SCHEMA,
  deleteChangeLogDB,
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
      ("watermark", "pos", "change", "precommit", "writeTimeMs")
      VALUES (?, ?, ?, ?, ?)
  `);
  stmt.run(watermark, 0, '{"tag":"begin"}', null, null);
  for (let i = 1; i <= changes; i++) {
    stmt.run(watermark, i, `{"tag":"insert","pos":${i}}`, null, null);
  }
  stmt.run(watermark, changes + 1, '{"tag":"commit"}', precommit, writeTimeMs);
}

function seedRows(anchor: ChangeLogAnchor) {
  return [
    {
      watermark: anchor.resumeWatermark,
      pos: 0,
      change: '{"tag":"begin"}',
      precommit: null,
      writeTimeMs: null,
    },
    {
      watermark: anchor.resumeWatermark,
      pos: 1,
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
          name: 'change',
          type: 'TEXT',
          notnull: 1,
          dflt_value: null,
          pk: 0,
        },
        {
          cid: 3,
          name: 'precommit',
          type: 'TEXT',
          notnull: 0,
          dflt_value: null,
          pk: 0,
        },
        {
          cid: 4,
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
        {seqno: 0, cid: 4, name: 'writeTimeMs'},
        {seqno: 1, cid: 0, name: 'watermark'},
      ]);
    });

    // The replica carried this same table at schema v14. The two definitions
    // describe different databases now and are deliberately not shared, so
    // this is the check that the writer's table is still the one a v14 replica
    // would have held — i.e. that the split did not silently change the shape.
    test('matches the shape frozen in replica migration 14', () => {
      using changeLog = new Database(lc, ':memory:');
      changeLog.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
      using replica = new Database(lc, ':memory:');
      replica.exec(CREATE_V14_CHANGE_LOG_STREAM);

      const schema = (db: Database) =>
        db
          .prepare(/*sql*/ `SELECT "type", "name", "tbl_name", "sql" FROM "sqlite_master"
                       ORDER BY "type", "name"`)
          .all();
      expect(schema(changeLog)).toEqual(schema(replica));
    });

    // The whole v2 shape lands in one bump, so that slice 8 can add the
    // `auto_vacuum` pragma without forcing a second reseed on every task.
    test('the meta row carries the identity triple and the seed point', () => {
      using db = createReconciledLog();

      expect(readChangeLogMeta(db)).toEqual({
        epoch: null,
        generation: '01',
        replicaID: '1777575698286',
        schemaVersion: 2,
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
        });
        expect(open.pragma('journal_mode')).toEqual([{journal_mode: 'wal2'}]);
        expect(readChangeLogHead(open)).toBe(ANCHOR.resumeWatermark);
      }

      const reopened = openChangeLogDBForWriting(lc, file.path, ANCHOR);
      using db = reopened.db;
      expect(reopened.result).toEqual({
        action: 'none',
        head: ANCHOR.resumeWatermark,
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
      });
      expectSeededAt(rebuilt, ANCHOR);
      expect(rebuilt.pragma('journal_mode')).toEqual([{journal_mode: 'wal2'}]);
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
      });
      expectSeededAt(db, anchor);
    });

    test('identity: a null epoch and replica ID match only another null', () => {
      const identity = {epoch: null, generation: '01', replicaID: null};
      using db = createReconciledLog(anchorAt('05', {identity}));

      expect(reconcileChangeLog(lc, db, anchorAt('05', {identity}))).toEqual({
        action: 'none',
        head: '05',
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
          change: '{"tag":"begin"}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '05',
          pos: 1,
          change: '{"tag":"insert","pos":1}',
          precommit: null,
          writeTimeMs: null,
        },
        {
          watermark: '05',
          pos: 2,
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
      });
      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'none',
        head: ANCHOR.resumeWatermark,
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
});
