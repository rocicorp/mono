import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, vi} from 'vitest';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {QueryResponse} from '../../../../zero-protocol/src/custom-queries.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../../zero-protocol/src/error-reason.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {
  ISSUES_QUERY,
  nextPoke,
  permissionsAll,
  setup,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import type {ViewSyncerService} from './view-syncer.ts';
import {type SyncContext} from './view-syncer.ts';

function scheduled401(queryIDs: string[]) {
  return {
    kind: ErrorKind.TransformFailed,
    message: 'Fetch from API server returned non-OK status 401',
    origin: ErrorOrigin.ZeroCache,
    queryIDs,
    reason: ErrorReason.HTTP,
    status: 401,
    bodyPreview: '{ "error": "Unauthorized" }',
  } as const;
}

function scheduled500(queryIDs: string[]) {
  return {
    kind: ErrorKind.TransformFailed,
    message: 'Fetch from API server returned non-OK status 500',
    origin: ErrorOrigin.ZeroCache,
    queryIDs,
    reason: ErrorReason.HTTP,
    status: 500,
    bodyPreview: '{ "error": "Internal Server Error" }',
  } as const;
}

const MAINTENANCE_INTERVAL_MS = 67_000;

function validationSuccess(userID: string | null = null): QueryResponse {
  return {
    kind: 'QueryResponse' as const,
    userID,
    queries: [],
  };
}

describe('view-syncer/auth maintenance', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  function callNextSetTimeout(
    setTimeoutFn: ReturnType<typeof vi.fn<typeof setTimeout>>,
    delta: number,
  ) {
    vi.setSystemTime(Date.now() + delta);
    const matchingCall = [...setTimeoutFn.mock.calls]
      .toReversed()
      .find(([, delay]) => delay === delta);
    const fn = matchingCall?.[0] ?? setTimeoutFn.mock.lastCall?.[0];
    expect(fn).toBeTypeOf('function');
    fn?.();
  }

  const SYNC_CONTEXT: SyncContext = {
    clientID: 'foo',
    profileID: 'p0000g00000003203',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    httpCookie: undefined,
    origin: undefined,
    userID: 'user-1',
    auth: undefined,
  };

  describe('scheduled revalidation', () => {
    let replicaDbFile: DbFile;
    let cvrDB: PostgresDB;
    let upstreamDb: PostgresDB;
    let stateChanges: Subscription<ReplicaState>;
    let vs: ViewSyncerService;
    let viewSyncerDone: Promise<void>;
    let connect: (
      ctx: SyncContext,
      desiredQueriesPatch: UpQueriesPatch,
    ) => Queue<Downstream>;
    let setTimeoutFn: ReturnType<typeof vi.fn<typeof setTimeout>>;
    let clearMocks: () => void;
    let customQueryTransformer: Awaited<
      ReturnType<typeof setup>
    >['customQueryTransformer'];

    beforeEach<PgTest>(async ({testDBs}) => {
      vi.setSystemTime(Date.UTC(2025, 0, 1));
      ({
        replicaDbFile,
        cvrDB,
        upstreamDb,
        stateChanges,
        vs,
        viewSyncerDone,
        connect,
        setTimeoutFn,
        customQueryTransformer,
        clearMocks,
      } = await setup(
        testDBs,
        'view_syncer_auth_maintenance_revalidate_test',
        permissionsAll,
        {
          authConfig: {
            revalidateIntervalSeconds: MAINTENANCE_INTERVAL_MS / 1000,
          },
          queryFetchMode: 'empty-validation',
        },
      ));

      return async () => {
        clearMocks();
        await vs.stop();
        await viewSyncerDone;
        await testDBs.drop(cvrDB, upstreamDb);
        replicaDbFile.delete();
      };
    });

    test('revalidates due validated connections', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));

      const authContext: SyncContext = {
        ...SYNC_CONTEXT,
        auth: {type: 'opaque', raw: 'token-1'},
      };
      const client = connect(authContext, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      ]);

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client);

      expect(validateSpy).toHaveBeenCalledTimes(1);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(() => expect(validateSpy).toHaveBeenCalledTimes(2), {
        timeout: 2_000,
      });
      expect(validateSpy.mock.calls[1][0].auth?.raw).toBe('token-1');
      expect(validateSpy.mock.calls[1][0].userID).toBe('user-1');
    });

    test('failed scheduled revalidation only fails the offending connection', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValueOnce(validationSuccess('user-1'))
        .mockResolvedValueOnce(validationSuccess('user-1'))
        .mockResolvedValueOnce(scheduled401([]))
        .mockResolvedValueOnce(validationSuccess('user-1'));

      const client1 = connect(
        {...SYNC_CONTEXT, auth: {type: 'opaque', raw: 'token-1'}},
        [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
      );
      const client2 = connect(
        {
          ...SYNC_CONTEXT,
          clientID: 'bar',
          wsID: 'ws2',
          auth: {type: 'opaque', raw: 'token-2'},
        },
        [{op: 'put', hash: 'query-hash2', ast: USERS_QUERY}],
      );

      await nextPoke(client1);
      await nextPoke(client2);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client1);
      await nextPoke(client2);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(
        async () =>
          await expect(client1.dequeue()).rejects.toThrow(
            'Fetch from API server returned non-OK status 401',
          ),
        {timeout: 2_000},
      );
      await vi.waitFor(() => expect(validateSpy).toHaveBeenCalledTimes(4), {
        timeout: 2_000,
      });
    });

    test('scheduled revalidation retries after transient query failure without disconnecting', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValueOnce(validationSuccess('user-1'))
        .mockResolvedValueOnce(scheduled500([]))
        .mockResolvedValueOnce(validationSuccess('user-1'));

      const client = connect(
        {...SYNC_CONTEXT, auth: {type: 'opaque', raw: 'token-1'}},
        [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
      );

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client);

      expect(validateSpy).toHaveBeenCalledTimes(1);
      expect(client.size()).toBe(0);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(() => expect(validateSpy).toHaveBeenCalledTimes(2), {
        timeout: 2_000,
      });
      expect(client.size()).toBe(0);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(() => expect(validateSpy).toHaveBeenCalledTimes(3), {
        timeout: 2_000,
      });
      expect(client.size()).toBe(0);
    });

    test('scheduled revalidation fails the connection on userID mismatch', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValueOnce(validationSuccess('user-1'))
        .mockResolvedValueOnce(validationSuccess('user-bad'));

      const client = connect(
        {...SYNC_CONTEXT, auth: {type: 'opaque', raw: 'token-1'}},
        [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
      );

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client);

      expect(validateSpy).toHaveBeenCalledTimes(1);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(
        async () =>
          await expect(client.dequeue()).rejects.toThrow(
            'Connection userID does not match validated server userID.',
          ),
        {timeout: 2_000},
      );
      expect(validateSpy).toHaveBeenCalledTimes(2);
    });

    test('ignores stale scheduled revalidation failures after auth changes', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      const staleValidation = resolver<ReturnType<typeof scheduled401>>();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValueOnce(validationSuccess('user-1'))
        .mockImplementationOnce(() => staleValidation.promise);

      const authContext: SyncContext = {
        ...SYNC_CONTEXT,
        auth: {type: 'opaque', raw: 'token-1'},
      };
      const selector = {
        clientID: authContext.clientID,
        wsID: authContext.wsID,
      };
      const client = connect(authContext, [
        {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      ]);

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client);

      expect(validateSpy).toHaveBeenCalledTimes(1);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(() => expect(validateSpy).toHaveBeenCalledTimes(2), {
        timeout: 2_000,
      });
      expect(validateSpy.mock.calls[1][0].auth?.raw).toBe('token-1');

      await vs.contextManager.updateAuth(selector, {auth: 'token-2'});

      // resolve the stale validation for the original auth
      staleValidation.resolve(scheduled401([]));

      await Promise.resolve();

      expect(vs.contextManager.getConnectionContext(selector)).toMatchObject({
        clientID: selector.clientID,
        wsID: selector.wsID,
        revision: 2,
        state: 'provisional',
      });
    });
  });

  describe('scheduled background retransform', () => {
    let replicaDbFile: DbFile;
    let cvrDB: PostgresDB;
    let upstreamDb: PostgresDB;
    let stateChanges: Subscription<ReplicaState>;
    let vs: ViewSyncerService;
    let viewSyncerDone: Promise<void>;
    let connect: (
      ctx: SyncContext,
      desiredQueriesPatch: UpQueriesPatch,
    ) => Queue<Downstream>;
    let setTimeoutFn: ReturnType<typeof vi.fn<typeof setTimeout>>;
    let clearMocks: () => void;
    let customQueryTransformer: Awaited<
      ReturnType<typeof setup>
    >['customQueryTransformer'];

    beforeEach<PgTest>(async ({testDBs}) => {
      vi.setSystemTime(Date.UTC(2025, 0, 1));
      ({
        replicaDbFile,
        cvrDB,
        upstreamDb,
        stateChanges,
        vs,
        viewSyncerDone,
        connect,
        setTimeoutFn,
        customQueryTransformer,
        clearMocks,
      } = await setup(
        testDBs,
        'view_syncer_auth_maintenance_retransform_test',
        permissionsAll,
        {
          authConfig: {
            retransformIntervalSeconds: MAINTENANCE_INTERVAL_MS / 1000,
          },
          queryFetchMode: 'empty-validation',
        },
      ));

      return async () => {
        clearMocks();
        await vs.stop();
        await viewSyncerDone;
        await testDBs.drop(cvrDB, upstreamDb);
        replicaDbFile.delete();
      };
    });

    test('retries scheduled background retransform with a promoted replacement connection', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));
      using transformSpy = vi
        .spyOn(transformer!, 'transform')
        .mockResolvedValueOnce({
          result: [
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ],
          cached: false,
        })
        .mockResolvedValueOnce({
          result: [
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1b',
            },
          ],
          cached: false,
        })
        .mockResolvedValueOnce({
          result: scheduled401(['custom-1']),
          cached: false,
        })
        .mockResolvedValueOnce({
          result: [
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-2',
            },
          ],
          cached: false,
        });

      const selectedClient = connect(
        {...SYNC_CONTEXT, auth: {type: 'opaque', raw: 'token-selected'}},
        [
          {
            op: 'put',
            hash: 'custom-1',
            name: 'named-query-1',
            args: ['thing'],
          },
        ],
      );
      await nextPoke(selectedClient);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(selectedClient);

      const replacementClient = connect(
        {
          ...SYNC_CONTEXT,
          clientID: 'bar',
          wsID: 'ws2',
          auth: {type: 'opaque', raw: 'token-replacement'},
        },
        [
          {
            op: 'put',
            hash: 'custom-1',
            name: 'named-query-1',
            args: ['thing'],
          },
        ],
      );
      await nextPoke(replacementClient);

      expect(validateSpy).toHaveBeenCalledTimes(2);
      expect(transformSpy).toHaveBeenCalledTimes(2);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(
        async () =>
          await expect(selectedClient.dequeue()).rejects.toThrow(
            'Fetch from API server returned non-OK status 401',
          ),
        {timeout: 2_000},
      );
      await vi.waitFor(() => expect(transformSpy).toHaveBeenCalledTimes(4), {
        timeout: 2_000,
      });
      expect(transformSpy.mock.calls[1][0].auth?.raw).toBe('token-replacement');
      expect(transformSpy.mock.calls[1][0].userID).toBe('user-1');
      expect(transformSpy.mock.calls[2][0].auth?.raw).toBe('token-selected');
      expect(transformSpy.mock.calls[2][0].userID).toBe('user-1');
      expect(transformSpy.mock.calls[3][0].auth?.raw).toBe('token-replacement');
      expect(transformSpy.mock.calls[3][0].userID).toBe('user-1');
    });

    test('scheduled background retransform retries after transient query failure without disconnecting', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));
      using transformSpy = vi
        .spyOn(transformer!, 'transform')
        .mockResolvedValueOnce({
          result: [
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ],
          cached: false,
        })
        .mockResolvedValueOnce({
          result: scheduled500(['custom-1']),
          cached: false,
        })
        .mockResolvedValueOnce({
          result: [
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ],
          cached: false,
        });

      const client = connect(
        {...SYNC_CONTEXT, auth: {type: 'opaque', raw: 'token-selected'}},
        [
          {
            op: 'put',
            hash: 'custom-1',
            name: 'named-query-1',
            args: ['thing'],
          },
        ],
      );

      await nextPoke(client);
      stateChanges.push({state: 'version-ready'});
      await nextPoke(client);

      expect(validateSpy).toHaveBeenCalledTimes(1);
      expect(transformSpy).toHaveBeenCalledTimes(1);
      expect(client.size()).toBe(0);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(() => expect(transformSpy).toHaveBeenCalledTimes(2), {
        timeout: 2_000,
      });
      expect(client.size()).toBe(0);

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(() => expect(transformSpy).toHaveBeenCalledTimes(3), {
        timeout: 2_000,
      });
      expect(client.size()).toBe(0);
    });
  });
});
