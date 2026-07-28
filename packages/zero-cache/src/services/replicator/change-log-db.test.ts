import {existsSync, writeFileSync} from 'node:fs';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {DbFile, expectTableExact} from '../../test/lite.ts';
import {
  CHANGE_LOG_DB_SCHEMA_VERSION,
  CHANGE_LOG_META_TABLE,
  changeLogFileName,
  deleteChangeLogDB,
  openChangeLogDB,
  readReplicaAnchor,
  reconcileChangeLog,
  type ReplicaAnchor,
} from './change-log-db.ts';
import {
  CHANGE_LOG_STREAM_TABLE,
  CREATE_CHANGE_LOG_STREAM_SCHEMA,
} from './schema/change-log-stream.ts';

const lc = createSilentLogContext();

const ANCHOR: ReplicaAnchor = {
  replicaVersion: '01',
  stateVersion: '05',
  writeTimeMs: 12345,
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

type PartialAnchor = Omit<Partial<ReplicaAnchor>, 'writeTimeMs'> & {
  writeTimeMs?: number | null | undefined;
};

function createReplica(anchor: PartialAnchor = ANCHOR): Database {
  const db = new Database(lc, ':memory:');
  db.exec(/*sql*/ `
    CREATE TABLE "_zero.replicationConfig" (
      replicaVersion TEXT NOT NULL,
      publications TEXT NOT NULL,
      initialSyncContext TEXT DEFAULT '{}',
      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
    );
    CREATE TABLE "_zero.replicationState" (
      stateVersion TEXT NOT NULL,
      writeTimeMs INTEGER,
      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
    );
  `);
  if (anchor.replicaVersion !== undefined) {
    db.prepare(/*sql*/ `
      INSERT INTO "_zero.replicationConfig" (replicaVersion, publications)
        VALUES (?, '[]')
    `).run(anchor.replicaVersion);
  }
  if (anchor.stateVersion !== undefined) {
    db.prepare(/*sql*/ `
      INSERT INTO "_zero.replicationState" (stateVersion, writeTimeMs)
        VALUES (?, ?)
    `).run(anchor.stateVersion, anchor.writeTimeMs ?? null);
  }
  return db;
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

function seedRows(anchor: ReplicaAnchor) {
  return [
    {
      watermark: anchor.stateVersion,
      pos: 0,
      change: '{"tag":"begin"}',
      precommit: null,
      writeTimeMs: null,
    },
    {
      watermark: anchor.stateVersion,
      pos: 1,
      change: '{"tag":"commit"}',
      precommit: anchor.stateVersion,
      writeTimeMs: anchor.writeTimeMs,
    },
  ];
}

function head(db: Database): string | null {
  return db
    .prepare(
      /*sql*/ `SELECT max("watermark") AS "head" FROM "${CHANGE_LOG_STREAM_TABLE}"`,
    )
    .get<{head: string | null}>().head;
}

function meta(db: Database) {
  return db.prepare(/*sql*/ `SELECT * FROM "${CHANGE_LOG_META_TABLE}"`).get();
}

/** A change-log db that is valid and consistent with {@link ANCHOR}. */
function createReconciledLog(anchor = ANCHOR): Database {
  const db = new Database(lc, ':memory:');
  reconcileChangeLog(lc, db, anchor);
  return db;
}

describe('replicator/change-log-db', () => {
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
      expect(head(db)).toBe(ANCHOR.stateVersion);
    });

    // Slice 7E turns this into a decline-and-fall-back-to-PG, rather than a
    // failed subscription.
    test('readonly open of an absent log throws', () => {
      const file = newDbFile();
      expect(() => openChangeLogDB(lc, file.path, {readonly: true})).toThrow();
    });
  });

  describe('readReplicaAnchor', () => {
    test('reads the replica facts the log is anchored to', () => {
      using replica = createReplica();
      expect(readReplicaAnchor(replica)).toEqual(ANCHOR);
    });

    test('requires initialized replication state', () => {
      using replica = createReplica({replicaVersion: '01'});
      expect(() => readReplicaAnchor(replica)).toThrow(
        'replication state must be initialized',
      );
    });

    test('requires an initialized writeTimeMs', () => {
      using replica = createReplica({...ANCHOR, writeTimeMs: null});
      expect(() => readReplicaAnchor(replica)).toThrow(
        'replication state writeTimeMs must be initialized',
      );
    });
  });

  describe('reconcileChangeLog: reseed reasons', () => {
    test('created: empty database', () => {
      using db = new Database(lc, ':memory:');

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.stateVersion,
        reason: 'created',
      });
      expect(head(db)).toBe(ANCHOR.stateVersion);
      expect(meta(db)).toEqual({
        replicaVersion: ANCHOR.replicaVersion,
        schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
        lock: 1,
      });
      expectTableExact(
        db,
        CHANGE_LOG_STREAM_TABLE,
        seedRows(ANCHOR),
        'number',
        'watermark',
        'pos',
      );
    });

    test('created: stream table without the meta table', () => {
      using db = new Database(lc, ':memory:');
      db.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
      appendTransaction(db, '05', '04', 3, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.stateVersion,
        reason: 'created',
      });
      // The pre-existing rows are wiped, not retained.
      expectTableExact(
        db,
        CHANGE_LOG_STREAM_TABLE,
        seedRows(ANCHOR),
        'number',
        'watermark',
        'pos',
      );
    });

    test('created: meta table without its row', () => {
      using db = createReconciledLog();
      db.exec(/*sql*/ `DELETE FROM "${CHANGE_LOG_META_TABLE}"`);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.stateVersion,
        reason: 'created',
      });
      expect(meta(db)).toEqual({
        replicaVersion: ANCHOR.replicaVersion,
        schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
        lock: 1,
      });
    });

    test('created: meta table without the stream table', () => {
      using db = createReconciledLog();
      db.exec(/*sql*/ `DROP TABLE "${CHANGE_LOG_STREAM_TABLE}"`);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.stateVersion,
        reason: 'created',
      });
      expect(head(db)).toBe(ANCHOR.stateVersion);
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
        head: ANCHOR.stateVersion,
        reason: 'schema-mismatch',
      });
      expect(meta(db)).toEqual({
        replicaVersion: ANCHOR.replicaVersion,
        schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
        lock: 1,
      });
      expectTableExact(
        db,
        CHANGE_LOG_STREAM_TABLE,
        seedRows(ANCHOR),
        'number',
        'watermark',
        'pos',
      );
    });

    test('replica-version-mismatch', () => {
      // A log left behind by a replica that was reset and re-synced.
      using db = createReconciledLog({...ANCHOR, replicaVersion: '00'});
      appendTransaction(db, '07', '05', 3, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.stateVersion,
        reason: 'replica-version-mismatch',
      });
      expect(meta(db)).toEqual({
        replicaVersion: ANCHOR.replicaVersion,
        schemaVersion: CHANGE_LOG_DB_SCHEMA_VERSION,
        lock: 1,
      });
      expectTableExact(
        db,
        CHANGE_LOG_STREAM_TABLE,
        seedRows(ANCHOR),
        'number',
        'watermark',
        'pos',
      );
    });

    test('gap: valid meta with an empty log', () => {
      using db = createReconciledLog();
      db.exec(/*sql*/ `DELETE FROM "${CHANGE_LOG_STREAM_TABLE}"`);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.stateVersion,
        reason: 'gap',
      });
      expect(head(db)).toBe(ANCHOR.stateVersion);
    });

    test('gap: head behind the replica', () => {
      // e.g. the replica advanced while sqliteChangeLogMode was off, or power
      // was lost with synchronous=NORMAL on the log.
      using db = createReconciledLog({...ANCHOR, stateVersion: '03'});
      appendTransaction(db, '04', '03', 2, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.stateVersion,
        reason: 'gap',
      });
      expectTableExact(
        db,
        CHANGE_LOG_STREAM_TABLE,
        seedRows(ANCHOR),
        'number',
        'watermark',
        'pos',
      );
    });

    test('gap: truncation leaves the head behind the replica', () => {
      using db = createReconciledLog({...ANCHOR, stateVersion: '03'});
      appendTransaction(db, '07', '03', 2, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'reseeded',
        head: ANCHOR.stateVersion,
        reason: 'gap',
      });
      expect(head(db)).toBe(ANCHOR.stateVersion);
    });
  });

  describe('reconcileChangeLog: truncation', () => {
    test('removes rows above the replica head and retains the rest', () => {
      using db = createReconciledLog({...ANCHOR, stateVersion: '02'});
      appendTransaction(db, '03', '02', 1, 100);
      appendTransaction(db, '05', '03', 1, 200);
      appendTransaction(db, '06', '05', 1, 300);
      appendTransaction(db, '07', '06', 2, 400);

      // 3 rows in '06' + 4 rows in '07'
      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'truncated',
        head: ANCHOR.stateVersion,
        rows: 7,
      });
      expect(head(db)).toBe(ANCHOR.stateVersion);
      expect(
        db
          .prepare(/*sql*/ `SELECT DISTINCT "watermark" FROM "${CHANGE_LOG_STREAM_TABLE}"
                       ORDER BY "watermark"`)
          .all(),
      ).toEqual([{watermark: '02'}, {watermark: '03'}, {watermark: '05'}]);
      // The log's new head transaction is untouched by the truncation.
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

    test('removes a phantom transaction whole', () => {
      // Every row of a transaction carries its commit watermark, so a phantom
      // cannot leave an orphaned begin or a mid-transaction row behind.
      using db = createReconciledLog();
      appendTransaction(db, '09', '05', 25, 100);

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'truncated',
        head: ANCHOR.stateVersion,
        rows: 27,
      });
      expect(
        db
          .prepare(/*sql*/ `SELECT count(*) AS "rows" FROM "${CHANGE_LOG_STREAM_TABLE}"
                       WHERE "watermark" > @stateVersion`)
          .get<{rows: number}>({stateVersion: ANCHOR.stateVersion}).rows,
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
        head: ANCHOR.stateVersion,
      });
      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'none',
        head: ANCHOR.stateVersion,
      });
    });
  });

  describe('reconcileChangeLog: consistent log', () => {
    test('an empty-but-valid log at the replica head is a no-op', () => {
      using db = createReconciledLog();

      expect(reconcileChangeLog(lc, db, ANCHOR)).toEqual({
        action: 'none',
        head: ANCHOR.stateVersion,
      });
      expectTableExact(
        db,
        CHANGE_LOG_STREAM_TABLE,
        seedRows(ANCHOR),
        'number',
        'watermark',
        'pos',
      );
    });

    test('a log at the replica head is not written to', () => {
      const file = newDbFile();
      using db = openChangeLogDB(lc, file.path, {readonly: false});
      reconcileChangeLog(lc, db, ANCHOR);
      appendTransaction(db, '07', '05', 2, 100);
      const state = {...ANCHOR, stateVersion: '07'};

      // data_version only advances for writes made by *another* connection.
      using observer = openChangeLogDB(lc, file.path, {readonly: true});
      const dataVersion = () => observer.pragma('data_version');
      const before = dataVersion();

      expect(reconcileChangeLog(lc, db, state)).toEqual({
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
      // The phantom is still there: nothing was half-applied.
      expect(head(db)).toBe('09');
    });

    test('leaves no transaction open when a reseed fails', () => {
      const file = newDbFile();
      {
        using db = openChangeLogDB(lc, file.path, {readonly: false});
        reconcileChangeLog(lc, db, {...ANCHOR, replicaVersion: '00'});
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
