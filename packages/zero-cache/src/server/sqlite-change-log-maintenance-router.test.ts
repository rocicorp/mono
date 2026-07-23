import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {
  requestSQLiteChangeLogMaintenance,
  type SQLiteChangeLogMaintenanceRequest,
  type SQLiteChangeLogMaintenanceResponse,
} from '../services/replicator/sqlite-change-log-maintenance.ts';
import type {SQLiteChangeLogPurgeResult} from '../services/replicator/sqlite-change-log-purger.ts';
import {inProcChannel, type Worker} from '../types/processes.ts';
import {SQLiteChangeLogMaintenanceRouter} from './sqlite-change-log-maintenance-router.ts';

const lc = createSilentLogContext();
const maintenance = {
  safeFloor: '06',
  requestTimeMs: 10_000,
  retentionMs: 1000,
  maxRows: 100,
} as const;

const result: SQLiteChangeLogPurgeResult = {
  headWatermark: '08',
  timeFloor: '08',
  effectiveFloor: '06',
  deletedRows: 4,
  deletedBeforeWatermark: '06',
  moreEligible: false,
};

const routers: SQLiteChangeLogMaintenanceRouter[] = [];

afterEach(() => {
  for (const router of routers.splice(0)) {
    router.close();
  }
});

function channels(): {
  changeStreamer: Worker;
  dispatcherChangeStreamer: Worker;
  dispatcherReplicator: Worker;
  replicator: Worker;
} {
  const [changeStreamer, dispatcherChangeStreamer] = inProcChannel();
  const [dispatcherReplicator, replicator] = inProcChannel();
  return {
    changeStreamer,
    dispatcherChangeStreamer,
    dispatcherReplicator,
    replicator,
  };
}

describe('server/sqlite-change-log-maintenance-router', () => {
  test('routes a typed request and response through the canonical worker', async () => {
    const {
      changeStreamer,
      dispatcherChangeStreamer,
      dispatcherReplicator,
      replicator,
    } = channels();
    routers.push(
      new SQLiteChangeLogMaintenanceRouter(
        lc,
        dispatcherChangeStreamer,
        dispatcherReplicator,
      ),
    );
    replicator.onMessageType<SQLiteChangeLogMaintenanceRequest>(
      'sqliteChangeLogMaintenanceRequest',
      request => {
        replicator.send<SQLiteChangeLogMaintenanceResponse>([
          'sqliteChangeLogMaintenanceResponse',
          {requestID: request.requestID, result},
        ]);
      },
    );

    await expect(
      requestSQLiteChangeLogMaintenance(changeStreamer, maintenance, 1000),
    ).resolves.toEqual(result);
  });

  test('returns an error when no canonical worker is available', async () => {
    const {changeStreamer, dispatcherChangeStreamer} = channels();
    routers.push(
      new SQLiteChangeLogMaintenanceRouter(
        lc,
        dispatcherChangeStreamer,
        undefined,
      ),
    );

    await expect(
      requestSQLiteChangeLogMaintenance(changeStreamer, maintenance, 1000),
    ).rejects.toThrow('canonical SQLite change-log writer is unavailable');
  });

  test('rejects malformed request payloads before routing', async () => {
    const {
      changeStreamer,
      dispatcherChangeStreamer,
      dispatcherReplicator,
      replicator,
    } = channels();
    routers.push(
      new SQLiteChangeLogMaintenanceRouter(
        lc,
        dispatcherChangeStreamer,
        dispatcherReplicator,
      ),
    );
    const response = new Promise<SQLiteChangeLogMaintenanceResponse[1]>(
      resolve => {
        changeStreamer.onceMessageType<SQLiteChangeLogMaintenanceResponse>(
          'sqliteChangeLogMaintenanceResponse',
          resolve,
        );
      },
    );
    let routed = false;
    replicator.onMessageType<SQLiteChangeLogMaintenanceRequest>(
      'sqliteChangeLogMaintenanceRequest',
      () => {
        routed = true;
      },
    );

    changeStreamer.send([
      'sqliteChangeLogMaintenanceRequest',
      {
        requestID: 'invalid-1',
        safeFloor: '',
        requestTimeMs: 10_000,
        retentionMs: 1000,
        maxRows: 100,
      },
    ] as SQLiteChangeLogMaintenanceRequest);

    await expect(response).resolves.toMatchObject({
      requestID: 'invalid-1',
      error: {message: expect.stringContaining('safe floor')},
    });
    expect(routed).toBe(false);
  });

  test('fails an in-flight request when the canonical worker closes', async () => {
    const {changeStreamer, dispatcherChangeStreamer, dispatcherReplicator} =
      channels();
    routers.push(
      new SQLiteChangeLogMaintenanceRouter(
        lc,
        dispatcherChangeStreamer,
        dispatcherReplicator,
      ),
    );

    const pending = requestSQLiteChangeLogMaintenance(
      changeStreamer,
      maintenance,
      1000,
    );
    dispatcherReplicator.emit('close', 1);

    await expect(pending).rejects.toThrow(
      'canonical SQLite change-log writer closed',
    );
  });

  test('times out when the canonical worker does not respond', async () => {
    const {changeStreamer, dispatcherChangeStreamer, dispatcherReplicator} =
      channels();
    routers.push(
      new SQLiteChangeLogMaintenanceRouter(
        lc,
        dispatcherChangeStreamer,
        dispatcherReplicator,
      ),
    );

    await expect(
      requestSQLiteChangeLogMaintenance(changeStreamer, maintenance, 10),
    ).rejects.toThrow('timed out waiting 10 ms');
  });
});
