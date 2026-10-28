import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {existsSync, mkdtempSync, rmSync, watch, writeFileSync} from 'node:fs';
import {access, writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {
  createLogContext,
  type LogConfig,
} from '../../../../shared/src/logging.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {initReplicationState} from '../../services/replicator/schema/replication-state.ts';
import {ThreadWriteWorkerClient} from '../../services/replicator/write-worker-client.ts';

const gate = process.env.ZERO_CHECKPOINT_STALL_DIR;
assert(gate, 'ZERO_CHECKPOINT_STALL_DIR must name an empty directory');
const directory = mkdtempSync(join(tmpdir(), 'serving-checkpoint-stall-'));
const log: LogConfig = {level: 'error', format: 'json'};
const db = new Database(createLogContext({log}), join(directory, 'stall.db'));
db.pragma('journal_mode = wal2');
db.pragma('wal_autocheckpoint = 0');
db.exec('CREATE TABLE events(id INTEGER PRIMARY KEY, payload BLOB)');
initReplicationState(db, ['zero_custom'], '00');
const insert = db.prepare('INSERT INTO events(payload) VALUES(zeroblob(8192))');
for (let i = 0; i < 3000; i++) insert.run();

const writer = new ThreadWriteWorkerClient();
const errors: Error[] = [];
writer.onError(error => errors.push(error));
const entered = join(gate, 'entered');
const release = join(gate, 'release');
execFileSync('mkfifo', [release]);
let released = false;
let committing:
  | ReturnType<ThreadWriteWorkerClient['processMessages']>
  | undefined;

async function beforeDeadline<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new Error('Replication did not progress during checkpoint sync'),
        ),
      10_000,
    );
  });
  try {
    return await Promise.race([promise, expired]);
  } finally {
    clearTimeout(timer);
  }
}

try {
  await writer.init(
    db.name,
    'serving',
    {busyTimeout: 30000, analysisLimit: 1000},
    log,
  );
  writeFileSync(join(gate, 'armed'), '');
  const reached = new Promise<void>((resolve, reject) => {
    const signal = AbortSignal.timeout(10_000);
    const watcher = watch(gate, {signal}, async () => {
      try {
        await access(entered);
        watcher.close();
        resolve();
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== 'ENOENT') reject(error);
      }
    });
    signal.addEventListener(
      'abort',
      () => reject(new Error('Checkpoint did not reach disk sync')),
      {once: true},
    );
  });
  for (let i = 1; i <= 20; i++) {
    const watermark = String(i).padStart(2, '0');
    committing = writer.processMessages([
      ['begin', {tag: 'begin'}, {commitWatermark: watermark}],
      ['commit', {tag: 'commit'}, {watermark}],
    ]);
    const result = await beforeDeadline(committing);
    assert.equal(result?.watermark, watermark);
    if (i === 1) await reached;
  }
  assert.deepEqual(
    db.prepare('SELECT stateVersion FROM "_zero.replicationState"').get(),
    {stateVersion: '20'},
  );
  process.stdout.write(
    '20 commits completed while checkpoint sync was blocked\n',
  );
  await writeFile(release, 'x');
  released = true;
  assert.deepEqual(errors, []);
} finally {
  if (!released && existsSync(entered)) {
    await writeFile(release, 'x');
  }
  await committing;
  await writer.stop();
  db.close();
  rmSync(directory, {recursive: true, force: true});
}
