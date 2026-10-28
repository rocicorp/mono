import {execFile} from 'node:child_process';
import {once} from 'node:events';
import {mkdtempSync, rmSync, statSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {promisify} from 'node:util';
import {LogContext} from '@rocicorp/logger';
import {afterAll, expect, test} from 'vitest';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {
  ServingCheckpointer,
  type CheckpointResult,
} from './serving-checkpointer.ts';
import {ThreadWriteWorkerClient} from './write-worker-client.ts';

const directory = mkdtempSync(join(tmpdir(), 'serving-checkpoint-'));
const lc = new LogContext('error');
const logConfig: LogConfig = {level: 'error', format: 'json'};
afterAll(() => rmSync(directory, {recursive: true, force: true}));

test.skipIf(!process.env.ZERO_CHECKPOINT_STALL_DIR)(
  'replication commits while checkpoint disk sync is blocked',
  async () => {
    const result = await promisify(execFile)(process.execPath, [
      '--experimental-strip-types',
      fileURLToPath(new URL('../../test/checkpoint/stall.ts', import.meta.url)),
    ]);
    expect(result.stdout).toBe(
      '20 commits completed while checkpoint sync was blocked\n',
    );
  },
  30_000,
);

function database(name: string, journalMode = 'wal2') {
  const path = join(directory, `${name}.db`);
  const db = new Database(lc, path);
  db.pragma(`journal_mode = ${journalMode}`);
  db.pragma('wal_autocheckpoint = 0');
  db.pragma('busy_timeout = 30000');
  db.exec('CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB)');
  return {db, path};
}

function payloadWriter(db: Database) {
  const begin = db.prepare('BEGIN IMMEDIATE');
  const insert = db.prepare(
    'INSERT INTO events(payload) VALUES(zeroblob(8192))',
  );
  const commit = db.prepare('COMMIT');
  return () => {
    begin.run();
    insert.run();
    commit.run();
  };
}

async function checkpoint(
  worker: ServingCheckpointer,
): Promise<CheckpointResult> {
  const completed = once(worker, 'checkpoint');
  worker.request();
  const [result] = await completed;
  return result;
}

test('coalesces requests and bounds WAL growth across repeated rotations', async () => {
  const {db, path} = database('rotations');
  const errors: Error[] = [];
  const worker = new ServingCheckpointer(path, logConfig, error =>
    errors.push(error),
  );
  let completed = 0;
  worker.on('checkpoint', () => completed++);
  try {
    await worker.ready;
    const insert = payloadWriter(db);
    for (let batch = 0; batch < 100; batch++) {
      for (let i = 0; i < 500; i++) {
        insert();
        worker.request();
      }
      await checkpoint(worker);
      await checkpoint(worker);
      const walBytes =
        statSync(`${path}-wal`).size + statSync(`${path}-wal2`).size;
      expect(walBytes).toBeLessThan(64 * 1024 * 1024);
    }
    expect(completed).toBeLessThan(400);
    expect(db.prepare('SELECT count(*) AS n FROM events').get()).toEqual({
      n: 50_000,
    });
    expect(errors).toEqual([]);
  } finally {
    await worker.close();
    db.close();
  }
}, 30_000);

test('readers do not block commits and checkpointing catches up after release', async () => {
  const {db, path} = database('readers');
  const reader = new Database(lc, path, {readonly: true});
  const errors: Error[] = [];
  const worker = new ServingCheckpointer(path, logConfig, error =>
    errors.push(error),
  );
  try {
    await worker.ready;
    reader.exec('BEGIN');
    expect(reader.prepare('SELECT count(*) AS n FROM events').get()).toEqual({
      n: 0,
    });
    const insert = payloadWriter(db);
    for (let i = 0; i < 3000; i++) insert();
    await checkpoint(worker);
    expect(reader.prepare('SELECT count(*) AS n FROM events').get()).toEqual({
      n: 0,
    });
    expect(db.prepare('SELECT count(*) AS n FROM events').get()).toEqual({
      n: 3000,
    });
    reader.exec('COMMIT');
    for (let batch = 0; batch < 5; batch++) {
      await checkpoint(worker);
      for (let i = 0; i < 500; i++) insert();
    }
    const result = await checkpoint(worker);
    expect(result.log - result.checkpointed).toBeLessThan(5000);
    expect(errors).toEqual([]);
  } finally {
    await worker.close();
    reader.close();
    db.close();
  }
  const reopened = new Database(lc, path);
  try {
    expect(reopened.prepare('SELECT count(*) AS n FROM events').get()).toEqual({
      n: 5500,
    });
    expect(reopened.pragma('integrity_check')).toEqual([
      {integrity_check: 'ok'},
    ]);
  } finally {
    reopened.close();
  }
});

test('startup failure preserves the SQLite error code', async () => {
  const errors: Error[] = [];
  const worker = new ServingCheckpointer(
    join(directory, 'missing.db'),
    logConfig,
    error => errors.push(error),
  );
  await expect(worker.ready).rejects.toMatchObject({
    cause: {code: 'SQLITE_CANTOPEN'},
  });
  await expect(worker.close()).rejects.toMatchObject({
    cause: {code: 'SQLITE_CANTOPEN'},
  });
  expect(errors).toHaveLength(1);
});

test.each(['serving', 'backup'] as const)(
  '%s writer commits, aborts, stops, and reopens',
  async mode => {
    const {db, path} = database(mode, mode === 'serving' ? 'wal2' : 'wal');
    initReplicationState(db, ['zero_custom'], '00');
    const writer = new ThreadWriteWorkerClient();
    const errors: Error[] = [];
    writer.onError(error => errors.push(error));
    try {
      await writer.init(
        path,
        mode,
        {
          busyTimeout: 30000,
          analysisLimit: 1000,
          walAutocheckpoint: mode === 'backup' ? 0 : undefined,
        },
        logConfig,
      );
      const result = await writer.processMessages([
        ['begin', {tag: 'begin'}, {commitWatermark: '01'}],
        ['commit', {tag: 'commit'}, {watermark: '01'}],
      ]);
      expect(result?.watermark).toBe('01');
      writer.abort();
      expect((await writer.getSubscriptionState()).watermark).toBe('01');
      expect(errors).toEqual([]);
    } finally {
      await writer.stop();
      db.close();
    }
    const reopened = new Database(lc, path);
    try {
      expect(
        reopened
          .prepare('SELECT stateVersion FROM "_zero.replicationState"')
          .get(),
      ).toEqual({stateVersion: '01'});
      expect(reopened.pragma('integrity_check')).toEqual([
        {integrity_check: 'ok'},
      ]);
    } finally {
      reopened.close();
    }
  },
);
