import {beforeEach, describe, expect, vi, type Mock} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {test, type PgTest} from '../../test/db.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {upstreamSchema} from '../../types/shards.ts';
import {id} from '../../types/sql.ts';
import {getMutationsTableDefinition} from '../change-source/pg/schema/shard.ts';
import {CVRStore} from './cvr-store.ts';
import {CVRConfigDrivenUpdater, CVRQueryDrivenUpdater} from './cvr.ts';
import {setupCVRTables} from './schema/cvr.ts';
import {type ClientQueryRecord} from './schema/types.ts';
import {ttlClockFromNumber} from './ttl-clock.ts';

const APP_ID = 'roze';
const SHARD_NUM = 1;
const SHARD = {appID: APP_ID, shardNum: SHARD_NUM};

describe('view-syncer/cvr-error-state', () => {
  const lc = createSilentLogContext();
  let db: PostgresDB;
  let upstreamDb: PostgresDB;
  let store: CVRStore;
  let setTimeoutFn: Mock<typeof setTimeout>;

  const TASK_ID = 'my-task';
  const CVR_ID = 'my-cvr';
  const CONNECT_TIME = Date.UTC(2024, 10, 22);
  const ON_FAILURE = (e: unknown) => {
    throw e;
  };

  beforeEach<PgTest>(async ({testDBs}) => {
    [db, upstreamDb] = await Promise.all([
      testDBs.create('view_syncer_cvr_error_schema'),
      testDBs.create('view_syncer_cvr_error_upstream'),
    ]);
    const shard = id(upstreamSchema(SHARD));
    await upstreamDb.begin(tx =>
      tx.unsafe(`
        CREATE SCHEMA IF NOT EXISTS ${shard};
        ${getMutationsTableDefinition(shard)}
      `),
    );
    await db.begin(tx => setupCVRTables(lc, tx, SHARD));

    // Initialize CVR
    await db.unsafe(`
    INSERT INTO "roze_1/cvr".instances ("clientGroupID", version, "lastActive", "ttlClock", "replicaVersion")
      VALUES('${CVR_ID}', '01', '2024-09-04T00:00:00Z', 
        (EXTRACT(EPOCH FROM TIMESTAMPTZ '2024-09-04T00:00:00Z') * 1000)::BIGINT, '01');
    INSERT INTO "roze_1/cvr"."rowsVersion" ("clientGroupID", version)
      VALUES('${CVR_ID}', '01');
      `);

    setTimeoutFn = vi.fn();
    store = new CVRStore(
      lc,
      db,
      upstreamDb,
      SHARD,
      TASK_ID,
      CVR_ID,
      ON_FAILURE,
      10,
      5,
      100,
      setTimeoutFn as unknown as typeof setTimeout,
    );

    return () => testDBs.drop(db, upstreamDb);
  });

  test('persist and load error state', async () => {
    const cvr = await store.load(lc, CONNECT_TIME);

    // Ensure query exists using ConfigDrivenUpdater
    const configUpdater = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    configUpdater.putDesiredQueries('client1', [
      {hash: 'q1', ast: {table: 'issues'}},
    ]);

    const {cvr: updatedCvr} = await configUpdater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    const updater = new CVRQueryDrivenUpdater(store, updatedCvr, '02', '01');

    // Track a query with an error
    updater.trackQueries(
      lc,
      [{id: 'q1', transformationHash: 'hash1', errorMessage: 'fail'}],
      [],
    );

    await updater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    // Verify DB state
    const queries =
      await db`SELECT * FROM "roze_1/cvr".queries WHERE "queryHash" = 'q1'`;
    expect(queries).toHaveLength(1);
    expect(queries[0].errorMessage).toBe('fail');
    expect(queries[0].errorVersion).toBe('02');

    // Load CVR and verify in-memory state
    const cvr2 = await store.load(lc, CONNECT_TIME);
    const q1 = cvr2.queries['q1'];
    expect(q1.errorMessage).toBe('fail');
    expect(q1.errorVersion).toEqual({stateVersion: '02'});
  });

  test('update existing query with error', async () => {
    let cvr = await store.load(lc, CONNECT_TIME);

    // Ensure query exists
    const configUpdater = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    configUpdater.putDesiredQueries('client1', [
      {hash: 'q1', ast: {table: 'issues'}},
    ]);

    const {cvr: updatedCvr} = await configUpdater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    let updater = new CVRQueryDrivenUpdater(store, updatedCvr, '02', '01');

    // Initial success
    updater.trackQueries(lc, [{id: 'q1', transformationHash: 'hash1'}], []);
    await updater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    // Update with error
    cvr = await store.load(lc, CONNECT_TIME);
    updater = new CVRQueryDrivenUpdater(store, cvr, '03', '01');
    updater.trackQueries(
      lc,
      [{id: 'q1', transformationHash: 'hash1', errorMessage: 'fail'}],
      [],
    );
    await updater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    const queries =
      await db`SELECT * FROM "roze_1/cvr".queries WHERE "queryHash" = 'q1'`;
    expect(queries[0].errorMessage).toBe('fail');
    expect(queries[0].errorVersion).toBe('03');

    // Update with same error (retry failed)
    cvr = await store.load(lc, CONNECT_TIME);
    updater = new CVRQueryDrivenUpdater(store, cvr, '04', '01');
    updater.trackQueries(
      lc,
      [{id: 'q1', transformationHash: 'hash1', errorMessage: 'fail'}],
      [],
    );
    await updater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    const queries2 =
      await db`SELECT * FROM "roze_1/cvr".queries WHERE "queryHash" = 'q1'`;
    expect(queries2[0].errorMessage).toBe('fail');
    expect(queries2[0].errorVersion).toBe('04');
  });

  test('clear error state on success', async () => {
    let cvr = await store.load(lc, CONNECT_TIME);

    // Ensure query exists
    const configUpdater = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    configUpdater.putDesiredQueries('client1', [
      {hash: 'q1', ast: {table: 'issues'}},
    ]);
    const {cvr: updatedCvr} = await configUpdater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    let updater = new CVRQueryDrivenUpdater(store, updatedCvr, '02', '01');

    // Initial error
    updater.trackQueries(
      lc,
      [{id: 'q1', transformationHash: 'hash1', errorMessage: 'fail'}],
      [],
    );
    await updater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    // Update with success (same hash)
    cvr = await store.load(lc, CONNECT_TIME);
    // Ensure query exists (again, because load creates new CVR)
    const configUpdater2 = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    configUpdater2.putDesiredQueries('client1', [
      {hash: 'q1', ast: {table: 'issues'}},
    ]);

    updater = new CVRQueryDrivenUpdater(store, cvr, '03', '01');
    updater.trackQueries(
      lc,
      [{id: 'q1', transformationHash: 'hash1'}], // No error
      [],
    );
    await store.flush(lc, cvr.version, cvr, Date.now());

    const queries =
      await db`SELECT * FROM "roze_1/cvr".queries WHERE "queryHash" = 'q1'`;
    expect(queries[0].errorMessage).toBeNull();
    // errorVersion should probably be preserved or cleared?
    // The implementation clears it if errorMessage is null.
    expect(queries[0].errorVersion).toBeNull();
  });

  test('persist retryErrorVersion in desires', async () => {
    const cvr = await store.load(lc, CONNECT_TIME);

    // Put desired query with retryErrorVersion
    // Use ConfigDrivenUpdater to handle it properly
    const configUpdater = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    configUpdater.putDesiredQueries('client1', [
      {
        hash: 'q1',
        ast: {table: 'issues'},
        retryErrorVersion: '01',
      },
    ]);

    await configUpdater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    const desires =
      await db`SELECT * FROM "roze_1/cvr".desires WHERE "queryHash" = 'q1'`;
    expect(desires).toHaveLength(1);
    expect(desires[0].retryErrorVersion).toBe('01');

    // Load CVR and verify
    const cvr2 = await store.load(lc, CONNECT_TIME);
    const q1 = cvr2.queries['q1'] as ClientQueryRecord;
    const clientState = q1.clientState['client1'];
    expect(clientState.retryErrorVersion).toEqual({stateVersion: '01'});
  });

  test('track error without transformationHash', async () => {
    let cvr = await store.load(lc, CONNECT_TIME);

    // Ensure query exists
    const configUpdater = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    configUpdater.putDesiredQueries('client1', [
      {hash: 'q1', ast: {table: 'issues'}},
    ]);
    const {cvr: updatedCvr} = await configUpdater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    const updater = new CVRQueryDrivenUpdater(store, updatedCvr, '02', '01');

    // Track a query with an error and no transformationHash
    updater.trackQueries(
      lc,
      [{id: 'q1', transformationHash: undefined, errorMessage: 'fail'}],
      [],
    );

    await updater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    // Verify DB state
    const queries =
      await db`SELECT * FROM "roze_1/cvr".queries WHERE "queryHash" = 'q1'`;
    expect(queries).toHaveLength(1);
    expect(queries[0].errorMessage).toBe('fail');
    expect(queries[0].errorVersion).toBe('02');
    // transformationHash should be null (it was never set)
    expect(queries[0].transformationHash).toBeNull();
  });

  test('retry query when errorVersion matches retryErrorVersion', async () => {
    let cvr = await store.load(lc, CONNECT_TIME);

    // 1. Setup: Query in error state
    const configUpdater = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    configUpdater.putDesiredQueries('client1', [
      {hash: 'q1', ast: {table: 'issues'}},
    ]);
    const {cvr: cvr1} = await configUpdater.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    const updater1 = new CVRQueryDrivenUpdater(store, cvr1, '02', '01');
    updater1.trackQueries(
      lc,
      [{id: 'q1', transformationHash: undefined, errorMessage: 'fail'}],
      [],
    );
    const {cvr: cvr2} = await updater1.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    // Verify error state
    let queries =
      await db`SELECT * FROM "roze_1/cvr".queries WHERE "queryHash" = 'q1'`;
    expect(queries[0].errorMessage).toBe('fail');
    expect(queries[0].errorVersion).toBe('02');

    // 2. Client requests retry for version '02'
    const configUpdater2 = new CVRConfigDrivenUpdater(store, cvr2, SHARD);
    configUpdater2.putDesiredQueries('client1', [
      {
        hash: 'q1',
        ast: {table: 'issues'},
        retryErrorVersion: '02',
      },
    ]);
    const {cvr: cvr3} = await configUpdater2.flush(
      lc,
      CONNECT_TIME,
      CONNECT_TIME,
      ttlClockFromNumber(CONNECT_TIME),
    );

    // 3. Verify that ViewSyncer would see this as a retry
    // We can't easily test ViewSyncer logic directly here without mocking,
    // but we can verify the CVR state is correct for ViewSyncer to consume.
    const q1 = cvr3.queries['q1'];
    if (q1.type !== 'client') throw new Error('Expected client query');
    const clientState = q1.clientState['client1'];
    expect(clientState.retryErrorVersion).toEqual({stateVersion: '02'});
    expect(q1.errorVersion).toEqual({stateVersion: '02'});
    // ViewSyncer logic: errorVersion === retryErrorVersion -> retry = true
  });
});
