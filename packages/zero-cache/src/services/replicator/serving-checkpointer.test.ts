import {mkdtempSync, rmSync, statSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {afterAll, expect, test} from 'vitest';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {ServingCheckpointer} from './serving-checkpointer.ts';
import {ThreadWriteWorkerClient} from './write-worker-client.ts';

const directory = mkdtempSync(join(tmpdir(), 'serving-checkpointer-'));
const lc = new LogContext('error');
const logConfig: LogConfig = {level: 'error', format: 'json'};
afterAll(() => rmSync(directory, {recursive: true, force: true}));

function database(name: string, journalMode: 'wal' | 'wal2') {
  const path = join(directory, `${name}.db`);
  const db = new Database(lc, path);
  db.pragma(`journal_mode = ${journalMode}`);
  db.pragma('wal_autocheckpoint = 0');
  db.exec('CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB)');
  const insert = db.prepare(
    'INSERT INTO events(payload) VALUES(zeroblob(8192))',
  );
  return {db, path, insert: () => insert.run()};
}

// In wal2 mode, `log` counts the frames in both wal files and `checkpointed`
// is non-zero only once the wal file the writer is not appending to has been
// checkpointed (every wal switch resets it).
function walStatus(db: Database) {
  const [status] = db.pragma<{log: number; checkpointed: number}>(
    'wal_checkpoint(NOOP)',
  );
  return status;
}

test('bounds the wal while the writer never checkpoints', async () => {
  const {db, path, insert} = database('bounded', 'wal2');
  const errors: Error[] = [];
  const checkpointer = new ServingCheckpointer(path, logConfig, e =>
    errors.push(e),
  );
  try {
    await checkpointer.ready();
    for (let batch = 0; batch < 20; batch++) {
      for (let i = 0; i < 500; i++) {
        insert();
        checkpointer.requestCheckpoint();
      }
      await expect
        .poll(() => walStatus(db).checkpointed, {timeout: 10_000})
        .toBeGreaterThan(0);
      // A few wal switches' worth of frames. Without checkpoints the writer
      // could never switch back, and the wal would hold every frame written.
      expect(walStatus(db).log).toBeLessThan(8000);
    }
    expect(
      statSync(`${path}-wal`).size + statSync(`${path}-wal2`).size,
    ).toBeLessThan(64 * 1024 * 1024);
    expect(db.prepare('SELECT count(*) AS n FROM events').get()).toEqual({
      n: 10_000,
    });
    expect(errors).toEqual([]);
  } finally {
    await checkpointer.close();
    db.close();
  }
  const reopened = new Database(lc, path);
  expect(reopened.pragma('integrity_check')).toEqual([{integrity_check: 'ok'}]);
  reopened.close();
}, 30_000);

test('reports a failure to open the replica with its SQLite error code', async () => {
  const errors: Error[] = [];
  const checkpointer = new ServingCheckpointer(
    join(directory, 'missing.db'),
    logConfig,
    e => errors.push(e),
  );
  await expect(checkpointer.ready()).rejects.toMatchObject({
    cause: {code: 'SQLITE_CANTOPEN'},
  });
  expect(errors).toHaveLength(1);
  await checkpointer.close();
});

test.each([
  ['serving', 'wal2'],
  ['backup', 'wal'],
] as const)('%s write worker', async (mode, journalMode) => {
  const {db, path, insert} = database(mode, journalMode);
  initReplicationState(db, ['zero_custom'], '00');
  // Fill the wal past the switch threshold so that the write worker's first
  // commit switches wal files, leaving a checkpointable one behind.
  for (let i = 0; i < 1000; i++) insert();

  const writer = new ThreadWriteWorkerClient();
  const errors: Error[] = [];
  writer.onError(e => errors.push(e));
  try {
    await writer.init(path, mode, getPragmaConfig(), logConfig);
    const result = await writer.processMessages([
      ['begin', {tag: 'begin'}, {commitWatermark: '01'}],
      ['commit', {tag: 'commit'}, {watermark: '01'}],
    ]);
    expect(result?.watermark).toBe('01');
    if (mode === 'serving') {
      await expect
        .poll(() => walStatus(db).checkpointed, {timeout: 10_000})
        .toBeGreaterThan(0);
    } else {
      expect(walStatus(db).checkpointed).toBe(0);
    }
    writer.abort();
    expect((await writer.getSubscriptionState()).watermark).toBe('01');
    expect(errors).toEqual([]);
  } finally {
    await writer.stop();
    db.close();
  }
  const reopened = new Database(lc, path);
  expect(
    reopened.prepare('SELECT stateVersion FROM "_zero.replicationState"').get(),
  ).toEqual({stateVersion: '01'});
  expect(reopened.pragma('integrity_check')).toEqual([{integrity_check: 'ok'}]);
  reopened.close();
});
