import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorReason} from '../../../../zero-protocol/src/error-reason.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import {TestDBs} from '../../test/db.ts';
import {cvrSchema} from '../../types/shards.ts';
import {
  defaultClientSchema,
  serviceID,
  setup,
  SHARD,
} from './view-syncer-test-util.ts';
import type {SyncContext} from './view-syncer.ts';

const SELECTOR = {clientID: 'foo', wsID: 'ws1'} as const;

const SYNC_CONTEXT: SyncContext = {
  ...SELECTOR,
  profileID: 'p0000g00000003203',
  baseCookie: null,
  protocolVersion: PROTOCOL_VERSION,
  httpCookie: undefined,
  origin: undefined,
  userID: 'u1',
  auth: {type: 'opaque', raw: 'expired-token'},
};

const refuse = () => Promise.resolve(new Response('expired', {status: 401}));

describe('view-syncer/initConnection overtaken by updateAuth', () => {
  let testDBs: TestDBs;
  let harness: Awaited<ReturnType<typeof setup>>;

  beforeEach(async () => {
    testDBs = new TestDBs();
    harness = await setup(testDBs, 'init_connection_auth_update', undefined, {
      queryFetchMode: 'empty-validation',
    });
  });

  afterEach(async () => {
    harness.clearMocks();
    await harness.vs.stop();
    await harness.viewSyncerDone;
    await testDBs.drop(harness.cvrDB, harness.upstreamDb);
    await testDBs.end();
    harness.replica.close();
    harness.replicaDbFile.delete();
  });

  /**
   * Connects with the expired token and holds that validation request until
   * the client has sent `refreshedAuth`, handled as the syncer handles an
   * `updateAuth` message, then refuses it. Every later request goes to
   * `answerLaterRequest`.
   */
  async function connectWhileAuthUpdates(
    refreshedAuth: string,
    answerLaterRequest: typeof fetch,
  ): Promise<Queue<Downstream>> {
    const {vs, connectWithQueueAndSource} = harness;
    const firstValidationSent = resolver<void>();
    const refuseFirstValidation = resolver<void>();
    let requestCount = 0;
    vi.stubGlobal(
      'fetch',
      async (url: RequestInfo | URL, init?: RequestInit) => {
        requestCount++;
        if (requestCount > 1) {
          return answerLaterRequest(url, init);
        }
        firstValidationSent.resolve();
        await refuseFirstValidation.promise;
        return refuse();
      },
    );

    const {queue} = connectWithQueueAndSource(
      SYNC_CONTEXT,
      [],
      defaultClientSchema,
    );
    await firstValidationSent.promise;

    const {revision: initialRevision} =
      vs.connContextManager.mustGetConnectionContext(SELECTOR);
    const {revision} = await vs.connContextManager.updateAuth(SELECTOR, {
      auth: refreshedAuth,
    });
    const authUpdated = vs.updateAuth(
      SELECTOR,
      ['updateAuth', {auth: refreshedAuth}],
      revision !== initialRevision,
    );
    refuseFirstValidation.resolve();
    await authUpdated;
    return queue;
  }

  function selectInstances() {
    const {cvrDB} = harness;
    return cvrDB<{clientSchema: unknown}[]>`
      SELECT "clientSchema" FROM ${cvrDB(cvrSchema(SHARD))}.instances
        WHERE "clientGroupID" = ${serviceID}`;
  }

  test('stores the client schema once the newer credential validates', async () => {
    const harnessFetch = globalThis.fetch;
    await connectWhileAuthUpdates('refreshed-token', harnessFetch);

    expect(await selectInstances()).toEqual([
      {clientSchema: defaultClientSchema},
    ]);
    expect(
      harness.queryFetch.validationCalls[0]?.headers.get('Authorization'),
    ).toBe('Bearer refreshed-token');
  });

  test('fails the connection, storing no client group, when the newer credential is refused too', async () => {
    const queue = await connectWhileAuthUpdates('revoked-token', refuse);

    await expect(queue.dequeue()).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.TransformFailed,
        reason: ErrorReason.HTTP,
        status: 401,
      },
    });
    expect(await selectInstances()).toEqual([]);
  });
});
