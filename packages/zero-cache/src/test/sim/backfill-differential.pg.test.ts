// The differential gate for SimPG's backfill half (D2b): one scripted workload
// against real Postgres and against SimPG, each replicated into a replica
// through the real change source glue and `ChangeProcessor`, must leave the
// two replicas in the same canonical state. A divergence is a SimPG bug until
// shown otherwise.

import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {ChangeStream} from '../../services/change-source/change-source.ts';
import {initializePostgresChangeSource} from '../../services/change-source/pg/change-source.ts';
import type {
  ChangeStreamData,
  ChangeStreamMessage,
} from '../../services/change-source/protocol/current/downstream.ts';
import {initReplicationState} from '../../services/replicator/schema/replication-state.ts';
import {createChangeProcessor} from '../../services/replicator/test-utils.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {getConnectionURI, test, testDBs, type PgTest} from '../db.ts';
import {DbFile} from '../lite.ts';
import {
  canonicalReplicaState,
  type CanonicalReplicaState,
} from '../replica-state.ts';
import {SimChangeSource} from './sim-change-source.ts';
import {SimPG} from './sim-pg.ts';
import {Trace} from './trace.ts';

const APP_ID = 'bfd';
const ROWS = 6;
const WORKLOAD_TABLE = 't1';

describe('sim/backfill differential', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  const files: DbFile[] = [];

  beforeEach<PgTest>(async () => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('backfill_differential_upstream');
    // Tables created after the change source starts are replicated with DDL,
    // as SimPG replicates every table.
    await upstream.unsafe(
      /*sql*/ `CREATE PUBLICATION zero_data FOR TABLES IN SCHEMA public;`,
    );
    return async () => {
      files.splice(0).forEach(file => file.delete());
      await testDBs.drop(upstream);
    };
  }, 60_000);

  /**
   * Applies `stream` to `replica` until a backfill completes. Returns whether
   * one did before the stream went quiet.
   */
  async function applyUntilCompleted(
    stream: ChangeStream,
    replica: Database,
  ): Promise<void> {
    const processor = createChangeProcessor(replica);
    const downstream = new Queue<ChangeStreamMessage | 'done'>();
    void (async () => {
      try {
        for await (const msg of stream.changes) {
          downstream.enqueue(msg);
        }
      } catch {
        // Canceled at teardown.
      }
      downstream.enqueue('done');
    })();
    let completed = false;
    for (;;) {
      const msg = await downstream.dequeue('done', 30_000);
      if (msg === 'done') {
        throw new Error('the change stream went quiet before completing');
      }
      if (msg[0] === 'status' || msg[0] === 'control') {
        continue;
      }
      // Round-tripped through JSON, as the wire does.
      processor.processMessage(
        lc,
        BigIntJSON.parse(BigIntJSON.stringify(msg)) as ChangeStreamData,
      );
      completed ||= msg[0] === 'data' && msg[1].tag === 'backfill-completed';
      if (completed && msg[0] === 'commit') {
        return;
      }
    }
  }

  test('a column backfilled after inserts, an update and a delete', async () => {
    // Postgres.
    const pgFile = new DbFile('backfill_differential_pg');
    files.push(pgFile);
    const {changeSource} = await initializePostgresChangeSource(
      lc,
      getConnectionURI(upstream),
      {appID: APP_ID, publications: ['zero_data'], shardNum: 0},
      pgFile.path,
      {tableCopyWorkers: 1},
      {test: 'backfill-differential'},
      0,
      {},
      {backupV5: true},
      null,
      undefined,
      {resume: true, commitThresholdBytes: 64 * 1024},
    );
    const pgReplica = pgFile.connect(lc);
    const pgStream = await changeSource.startStream('00', []);
    await upstream.unsafe(/*sql*/ `
      CREATE TABLE t1 (id INT8 PRIMARY KEY, c2 TEXT);
      INSERT INTO t1 (id, c2) SELECT i, 'v' || i FROM generate_series(1, ${ROWS}) i;
    `);
    await upstream.unsafe(/*sql*/ `UPDATE t1 SET c2 = 'changed' WHERE id = 2`);
    await upstream.unsafe(/*sql*/ `DELETE FROM t1 WHERE id = ${ROWS}`);
    // A volatile default, which replication cannot deliver, that evaluates to
    // a value SimPG can match.
    await upstream.unsafe(
      /*sql*/ `ALTER TABLE t1 ADD COLUMN c3 INT8 DEFAULT (random() * 0)::int8`,
    );
    await applyUntilCompleted(pgStream, pgReplica);
    const pgState = canonicalReplicaState(pgReplica);
    pgStream.changes.cancel();
    await changeSource.stop();
    pgReplica.close();

    // SimPG.
    const simFile = new DbFile('backfill_differential_sim');
    files.push(simFile);
    const simReplica = simFile.connect(lc);
    const pg = new SimPG(new Trace({now: () => 0, runDirs: []}));
    initReplicationState(simReplica, ['zero_data'], pg.replicaVersion);
    let tx = pg.begin();
    tx.createTable(['text'], 't1');
    for (let id = 1; id <= ROWS; id++) {
      tx.insert('t1', {id, c2: `v${id}`});
    }
    pg.commit(tx, 1);
    tx = pg.begin();
    tx.update('t1', {id: 2}, {id: 2, c2: 'changed'});
    pg.commit(tx, 1);
    tx = pg.begin();
    tx.delete('t1', {id: ROWS});
    pg.commit(tx, 1);
    tx = pg.begin();
    tx.addColumn('t1', 'int8', 0, true);
    pg.commit(tx, 1);

    const source = new SimChangeSource(lc, pg, {
      batchRows: 2,
      commitThresholdBytes: 64 * 1024,
      resume: true,
    });
    const simStream = await source.startStream(pg.replicaVersion, []);
    pg.deliver(Number.MAX_SAFE_INTEGER);
    await applyUntilCompleted(simStream, simReplica);
    const simState = canonicalReplicaState(simReplica);
    simStream.changes.cancel();
    simReplica.close();

    expect(workloadState(simState)).toEqual(workloadState(pgState));
  }, 120_000);
});

/**
 * The part of a replica's canonical state that the workload made. A replica
 * synced from Postgres also holds the shard's own tables, which SimPG has no
 * counterpart for.
 */
function workloadState(state: CanonicalReplicaState) {
  const isWorkload = (table: string) => table === WORKLOAD_TABLE;
  return {
    physicalTables: state.physicalTables.filter(t => isWorkload(t.name)),
    logicalTables: state.logicalTables.filter(t => isWorkload(t.name)),
    indexes: state.indexes.filter(i => isWorkload(i.tableName)),
    columnMetadata: (state.columnMetadata as {table_name: string}[]).filter(c =>
      isWorkload(c.table_name),
    ),
    rows: state.rows.filter(r => isWorkload(r.name)),
    integrityCheck: state.integrityCheck,
  };
}
