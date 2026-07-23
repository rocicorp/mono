import {resolver} from '@rocicorp/resolver';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {SQLiteChangeLogMaintenanceRouter} from '../../server/sqlite-change-log-maintenance-router.ts';
import {DbFile} from '../../test/lite.ts';
import {inProcChannel} from '../../types/processes.ts';
import {Subscription} from '../../types/subscription.ts';
import {setUpMessageHandlers} from '../../workers/replicator.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {IncrementalSyncer} from '../replicator/incremental-sync.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {requestSQLiteChangeLogMaintenance} from '../replicator/sqlite-change-log-maintenance.ts';
import {ThreadWriteWorkerClient} from '../replicator/write-worker-client.ts';
import type {
  Downstream,
  SerializedDownstream,
  SubscriberContext,
} from './change-streamer.ts';
import {SQLiteChangeLogReader} from './sqlite-change-log-reader.ts';

const lc = createSilentLogContext();
const files: DbFile[] = [];

afterEach(() => {
  for (const file of files.splice(0)) {
    file.delete();
  }
});

function transaction(watermark: string): ChangeStreamData[] {
  return [
    ['begin', {tag: 'begin'}, {commitWatermark: watermark}],
    ['data', {tag: 'truncate', relations: []}],
    ['commit', {tag: 'commit'}, {watermark}],
  ];
}

describe('change-streamer/sqlite-change-log-maintenance integration', () => {
  test('applies live changes, reads catchup, and drains repeated routed purge batches', async () => {
    const file = new DbFile('sqlite-change-log-maintenance');
    files.push(file);
    const db = file.connect(lc);
    db.pragma('journal_mode = wal');
    initReplicationState(db, ['zero_data'], '02');

    const worker = new ThreadWriteWorkerClient();
    await worker.init(
      file.path,
      'serving',
      true,
      {busyTimeout: 30_000, analysisLimit: 1000},
      {level: 'error', format: 'text'},
    );
    const downstream = new Subscription<SerializedDownstream, Downstream>(
      {},
      data => ({data, json: BigIntJSON.stringify(data)}),
    );
    const subscribed = resolver<void>();
    const subscribe = vi.fn((_ctx: SubscriberContext) => {
      subscribed.resolve();
      return Promise.resolve(downstream);
    });
    const syncer = new IncrementalSyncer(
      lc,
      'task-id',
      'canonical-replicator',
      {subscribe},
      worker,
      'serving',
      null,
      undefined,
    );
    const syncing = syncer.run();
    const versions = syncer.subscribe()[Symbol.asyncIterator]();

    const [changeStreamerWorker, dispatcherChangeStreamer] = inProcChannel();
    const [dispatcherReplicator, replicatorWorker] = inProcChannel();
    using _router = new SQLiteChangeLogMaintenanceRouter(
      lc,
      dispatcherChangeStreamer,
      dispatcherReplicator,
    );
    setUpMessageHandlers(
      lc,
      {
        status: () => Promise.resolve({status: 'ok'}),
        subscribe: () => Subscription.create(),
        purgeChangeLog: maintenance => syncer.purgeChangeLog(maintenance),
      },
      replicatorWorker,
      true,
    );

    try {
      await versions.next();
      await subscribed.promise;
      const processMessage = vi.spyOn(worker, 'processMessage');
      for (const watermark of ['04', '06', '08']) {
        for (const message of transaction(watermark)) {
          downstream.push(message);
        }
      }
      await vi.waitFor(() => expect(processMessage).toHaveBeenCalledTimes(9));
      await vi.waitFor(() =>
        expect(
          db
            .prepare(`SELECT "stateVersion" FROM "_zero.replicationState"`)
            .get(),
        ).toEqual({stateVersion: '08'}),
      );

      using reader = new SQLiteChangeLogReader(lc, file.path);
      const plan = reader.plan('02');
      expect(plan).toEqual({
        kind: 'range',
        minWatermark: '02',
        headWatermark: '08',
      });
      if (plan.kind !== 'range') {
        throw new Error('expected catchable SQLite change-log range');
      }
      const caughtUp: string[] = [];
      for await (const batch of reader.read('02', plan.headWatermark, 2)) {
        caughtUp.push(...batch.map(([, tag]) => tag));
      }
      expect(caughtUp).toEqual([
        'begin',
        'truncate',
        'commit',
        'begin',
        'truncate',
        'commit',
        'begin',
        'truncate',
        'commit',
      ]);

      const batches = [];
      do {
        batches.push(
          await requestSQLiteChangeLogMaintenance(
            changeStreamerWorker,
            {
              safeFloor: '08',
              requestTimeMs: Number.MAX_SAFE_INTEGER,
              retentionMs: 1,
              maxRows: 3,
            },
            1000,
          ),
        );
      } while (batches.at(-1)?.moreEligible);

      expect(batches.map(({deletedRows}) => deletedRows)).toEqual([2, 3, 3]);
      expect(watermarks(db)).toEqual(['08']);
    } finally {
      downstream.cancel();
      await syncer.stop(lc);
      await syncing;
      await worker.stop();
      db.close();
    }
  });
});

function watermarks(db: Database): string[] {
  return db
    .prepare(/*sql*/ `
      SELECT DISTINCT "watermark"
      FROM "_zero.changeLogStream"
      ORDER BY "watermark"
    `)
    .all<{watermark: string}>()
    .map(({watermark}) => watermark);
}
