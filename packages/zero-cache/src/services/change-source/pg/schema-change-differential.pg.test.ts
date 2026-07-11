import type {LogContext} from '@rocicorp/logger';
import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {getConnectionURI, testDBs} from '../../../test/db.ts';
import {DbFile} from '../../../test/lite.ts';
import {canonicalReplicaState} from '../../../test/replica-state.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {Source} from '../../../types/streams.ts';
import {createChangeProcessor} from '../../replicator/test-utils.ts';
import type {ChangeStreamMessage} from '../protocol/current/downstream.ts';
import {initializePostgresChangeSource} from './change-source.ts';
import {initialSync} from './initial-sync.ts';

const APP_ID = 'schema_change_differential';
const PUBLICATION = 'schema_change_differential_publication';
const SHARD = {
  appID: APP_ID,
  shardNum: 0,
  publications: [PUBLICATION],
} as const;

test('streaming SET NOT NULL matches a fresh replica of the final PG schema', async () => {
  const lc = createSilentLogContext();
  const upstream = await testDBs.create('schema_change_differential');
  const replicaFile = new DbFile('schema-change-differential');
  let streamed: Database | undefined;
  let changes: Source<ChangeStreamMessage> | undefined;

  try {
    await setupUpstream(upstream);
    const source = (
      await initializePostgresChangeSource(
        lc,
        getConnectionURI(upstream),
        SHARD,
        replicaFile.path,
        {tableCopyWorkers: 1},
        {test: 'schema-change-differential'},
      )
    ).changeSource;
    const stream = await source.startStream('00');
    changes = stream.changes;

    streamed = replicaFile.connect(lc);
    const processor = createChangeProcessor(streamed);

    await upstream`
      ALTER TABLE foo ALTER COLUMN tenant_id SET NOT NULL
    `;
    await applyNextTransaction(lc, processor, changes);

    const rebuilt = new Database(lc, ':memory:');
    try {
      await initialSync(
        lc,
        SHARD,
        rebuilt,
        getConnectionURI(upstream),
        {
          tableCopyWorkers: 1,
          shadow: {sampleRate: 1, maxRowsPerTable: 100},
        },
        {test: 'schema-change-differential-reference'},
      );

      expect(canonicalReplicaState(streamed)).toEqual(
        canonicalReplicaState(rebuilt),
      );
    } finally {
      rebuilt.close();
    }
  } finally {
    changes?.cancel();
    streamed?.close();
    replicaFile.delete();
    await testDBs.drop(upstream);
  }
});

async function setupUpstream(upstream: PostgresDB) {
  await upstream.unsafe(/*sql*/ `
    CREATE TABLE foo (
      id INT8 PRIMARY KEY,
      tenant_id TEXT,
      group_id TEXT NOT NULL
    );
    CREATE INDEX foo_tenant_id_id ON foo (tenant_id, id);
    INSERT INTO foo (id, tenant_id, group_id)
      VALUES (1, 'tenant-1', 'group-1'), (2, 'tenant-2', 'group-2');
    CREATE PUBLICATION ${PUBLICATION} FOR TABLE foo;
  `);
}

async function applyNextTransaction(
  lc: LogContext,
  processor: ReturnType<typeof createChangeProcessor>,
  changes: Source<ChangeStreamMessage>,
) {
  let sawData = false;
  for await (const change of changes) {
    const [type] = change;
    if (type === 'control' || type === 'status') {
      continue;
    }
    processor.processMessage(lc, change);
    if (type === 'data') {
      sawData = true;
    } else if (type === 'commit' && sawData) {
      return;
    }
  }
  throw new Error('change stream ended before a schema transaction arrived');
}
