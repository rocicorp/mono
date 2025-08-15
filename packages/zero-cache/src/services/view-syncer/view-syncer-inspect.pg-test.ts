import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {Queue} from '../../../../shared/src/queue.ts';
import {type ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {testDBs} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {type FakeReplicator} from '../replicator/test-utils.ts';
import {
  expectNoPokes,
  ISSUES_QUERY,
  messages,
  nextPoke,
  permissionsAll,
  setup,
} from './view-syncer-test-util.ts';
import {type SyncContext, ViewSyncerService} from './view-syncer.ts';

describe('view-syncer/service', () => {
  let replicaDbFile: DbFile;
  let cvrDB: PostgresDB;
  let stateChanges: Subscription<ReplicaState>;

  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;
  let replicator: FakeReplicator;
  let connectWithQueueAndSource: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
    clientSchema?: ClientSchema,
    activeClients?: string[],
  ) => {
    queue: Queue<Downstream>;
    source: Source<Downstream>;
  };

  const SYNC_CONTEXT: SyncContext = {
    clientID: 'foo',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    schemaVersion: 2,
    tokenData: undefined,
    httpCookie: undefined,
  };

  beforeEach(async () => {
    ({
      replicaDbFile,
      cvrDB,
      stateChanges,
      vs,
      viewSyncerDone,
      replicator,
      connectWithQueueAndSource,
    } = await setup('view_syncer_inspect_test', permissionsAll));
  });

  afterEach(async () => {
    vi.useRealTimers();
    await vs.stop();
    await viewSyncerDone;
    await testDBs.drop(cvrDB);
    replicaDbFile.delete();
  });

  test('inspect metrics op returns server metrics', async () => {
    const {queue: client} = connectWithQueueAndSource(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);

    // Wait for initial hydration to complete
    await nextPoke(client);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);

    // Trigger query materializations to generate metrics
    replicator.processTransaction(
      'txn-1',
      messages.insert('issues', {
        id: 'test-issue',
        title: 'Test Issue',
        owner: '100',
        big: 1000,
        json: null,
        parent: null,
      }),
    );
    stateChanges.push({state: 'version-ready'});
    await expectNoPokes(client);

    // Now call the inspect method and expect it to send a response
    const inspectId = 'test-metrics-inspect';

    // Call inspect and wait for the response to come through the client queue
    await vs.inspect(SYNC_CONTEXT, ['inspect', {op: 'metrics', id: inspectId}]);

    const msg = await client.dequeue();
    expect(msg).toMatchObject([
      'inspect',
      {
        id: 'test-metrics-inspect',
        op: 'metrics',
        value: {
          'query-materialization-server': expect.arrayContaining([
            expect.any(Number),
          ]),
        },
      },
    ]);
  });

  test('inspect version', async () => {
    const {queue: client} = connectWithQueueAndSource(SYNC_CONTEXT, []);

    // Wait for initial hydration to complete
    await nextPoke(client);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);

    await expectNoPokes(client);

    const inspectId = 'test-version-inspect';

    // Call inspect and wait for the response to come through the client queue
    await vs.inspect(SYNC_CONTEXT, ['inspect', {op: 'version', id: inspectId}]);

    const msg = await client.dequeue();

    expect(msg).toEqual([
      'inspect',
      {
        id: 'test-version-inspect',
        op: 'version',
        value: expect.stringMatching(/^\d+\.\d+\.\d+$/),
      },
    ]);
  });
});
