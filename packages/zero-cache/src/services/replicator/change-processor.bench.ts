import {bench, describe, use} from '../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {DataOrSchemaChange} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor} from './change-processor.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {ReplicationMessages} from './test-utils.ts';

const ROWS = 1_000;
const lc = createSilentLogContext();
const messages = new ReplicationMessages({foo: ['id', 'shard']});

describe('change-processor DML apply', () => {
  const stableInsertChanges = transaction(
    '10',
    Array.from({length: ROWS}, (_, i) =>
      messages.insert('foo', {
        id: i,
        shard: i % 10,
        alpha: i * 2,
        beta: i * 3,
      }),
    ),
  );
  const stableInsertUncached = setupBenchmark(false);
  const stableInsertCached = setupBenchmark(true);

  bench(
    '1k stable-shape inserts (uncached plans)',
    () => {
      use(apply(stableInsertUncached, stableInsertChanges));
    },
    {min_cpu_time: 5e8, min_samples: 20},
  );

  bench(
    '1k stable-shape inserts (cached plans)',
    () => {
      use(apply(stableInsertCached, stableInsertChanges));
    },
    {min_cpu_time: 5e8, min_samples: 20},
  );

  const mixedDmlChanges = transaction('20', [
    ...Array.from({length: ROWS}, (_, i) =>
      messages.insert('foo', {
        id: i,
        shard: i % 10,
        alpha: i * 2,
        beta: i * 3,
      }),
    ),
    ...Array.from({length: ROWS}, (_, i) =>
      i % 2 === 0
        ? messages.update('foo', {
            id: i,
            shard: i % 10,
            alpha: i * 5,
            beta: i * 7,
          })
        : messages.update('foo', {
            beta: i * 7,
            shard: i % 10,
            id: i,
            alpha: i * 5,
          }),
    ),
    ...Array.from({length: ROWS}, (_, i) =>
      i % 2 === 0
        ? messages.delete('foo', {id: i, shard: i % 10})
        : messages.delete('foo', {shard: i % 10, id: i}),
    ),
  ]);
  const mixedDmlUncached = setupBenchmark(false);
  const mixedDmlCached = setupBenchmark(true);

  bench(
    '1k inserts + updates + deletes (uncached plans)',
    () => {
      use(apply(mixedDmlUncached, mixedDmlChanges));
    },
    {min_cpu_time: 5e8, min_samples: 20},
  );

  bench(
    '1k inserts + updates + deletes (cached plans)',
    () => {
      use(apply(mixedDmlCached, mixedDmlChanges));
    },
    {min_cpu_time: 5e8, min_samples: 20},
  );
});

function setupBenchmark(cacheDmlSqlPlans: boolean) {
  const replica = new Database(lc, ':memory:');
  initReplicationState(replica, ['zero_data'], '02');
  replica.exec(`
    CREATE TABLE foo(
      id INTEGER,
      shard INTEGER,
      alpha INTEGER,
      beta INTEGER,
      _0_version TEXT,
      PRIMARY KEY(id, shard)
    );
  `);
  return new ChangeProcessor(
    new StatementRunner(replica),
    'backup',
    (_, err) => {
      throw err;
    },
    {cacheDmlSqlPlans},
  );
}

function transaction(
  watermark: string,
  dataMessages: DataOrSchemaChange[],
): ChangeStreamData[] {
  return [
    ['begin', messages.begin(), {commitWatermark: watermark}],
    ...dataMessages.map(msg => ['data', msg] as ChangeStreamData),
    ['commit', messages.commit(), {watermark}],
  ];
}

function apply(processor: ChangeProcessor, changes: ChangeStreamData[]) {
  let result = null;
  for (const change of changes) {
    result = processor.processMessage(lc, change);
  }
  return result;
}
