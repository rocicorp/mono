import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, vi} from 'vitest';
import {Queue} from '../../../../shared/src/queue.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {ErrorOrigin} from '../../../../zero-protocol/src/error-origin.ts';
import {ErrorReason} from '../../../../zero-protocol/src/error-reason.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import type {
  HashedTransformResponse,
  TransformResponse,
} from '../../custom-queries/transform-query.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {ViewSyncerDownstream} from '../../types/downstream.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import type {ConnectionValidation} from './connection-context-manager.ts';
import {
  ISSUES_QUERY,
  nextPoke,
  permissionsAll,
  serviceID,
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

function validationSuccess(userID: string | null = null): TransformResponse {
  return {
    kind: 'QueryResponse' as const,
    validation: {
      kind: 'server-validated',
      validatedUserID: userID,
    },
    queries: [],
  };
}

const clientFallback: ConnectionValidation = {kind: 'client-fallback'};

function transformSuccess(
  result: Extract<HashedTransformResponse, {kind: 'success'}>['result'],
  validation: ConnectionValidation = clientFallback,
): HashedTransformResponse {
  return {
    kind: 'success' as const,
    result,
    cached: false as const,
    validation,
  };
}

function transformFailure(result: {
  kind: ErrorKind.TransformFailed;
  message: string;
  origin: ErrorOrigin.ZeroCache;
  queryIDs: string[];
  reason: ErrorReason.HTTP;
  status: number;
  bodyPreview: string;
}): HashedTransformResponse {
  return {
    kind: 'failed' as const,
    result,
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

  function hasScheduledTimeout(
    setTimeoutFn: ReturnType<typeof vi.fn<typeof setTimeout>>,
    delay: number,
  ) {
    return setTimeoutFn.mock.calls.some(
      ([, scheduledDelay]) => scheduledDelay === delay,
    );
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
      expect(validateSpy.mock.calls[1][0].user).toEqual({id: 'user-1'});
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

      await vs.connContextManager.updateAuth(selector, {auth: 'token-2'});

      // resolve the stale validation for the original auth
      staleValidation.resolve(scheduled401([]));

      await Promise.resolve();

      expect(
        vs.connContextManager.getConnectionContext(selector),
      ).toMatchObject({
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

    test('schedules shared retransform only after initial pipeline sync', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));
      using transformSpy = vi
        .spyOn(transformer!, 'transform')
        .mockResolvedValueOnce(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ]),
        )
        .mockResolvedValueOnce(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-2',
            },
          ]),
        );

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

      expect(validateSpy).toHaveBeenCalledTimes(1);
      expect(transformSpy).toHaveBeenCalledTimes(0);
      expect(hasScheduledTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS)).toBe(
        false,
      );

      stateChanges.push({state: 'version-ready'});
      await nextPoke(client);

      expect(transformSpy).toHaveBeenCalledTimes(1);
      await vi.waitFor(() => {
        expect(
          vs.connContextManager.getBackgroundConnectionContext(),
        ).toBeDefined();
        expect(
          vs.connContextManager.getGroupState().retransformAt,
        ).toBeDefined();
        expect(hasScheduledTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS)).toBe(
          true,
        );
      });

      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(() => expect(transformSpy).toHaveBeenCalledTimes(2), {
        timeout: 2_000,
      });
    });

    test('stale scheduled background retransform does not run after stop', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));
      using transformSpy = vi
        .spyOn(transformer!, 'transform')
        .mockResolvedValueOnce(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ]),
        );

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

      await vi.waitFor(() => {
        expect(validateSpy).toHaveBeenCalledTimes(1);
        expect(transformSpy).toHaveBeenCalledTimes(1);
        expect(
          vs.connContextManager.getGroupState().retransformAt,
        ).toBeDefined();
        expect(hasScheduledTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS)).toBe(
          true,
        );
      });

      await vs.stop();

      expect(
        vs.connContextManager.getGroupState().retransformAt,
      ).toBeUndefined();

      // Fire the previously scheduled maintenance callback after shutdown to
      // prove the stopped service no longer performs shared retransform work.
      callNextSetTimeout(setTimeoutFn, MAINTENANCE_INTERVAL_MS);

      await vi.waitFor(() => {
        expect(transformSpy).toHaveBeenCalledTimes(1);
        expect(
          vs.connContextManager.getGroupState().retransformAt,
        ).toBeUndefined();
      });
    });

    test('retries scheduled background retransform with a promoted replacement connection', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));
      using transformSpy = vi
        .spyOn(transformer!, 'transform')
        .mockResolvedValueOnce(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ]),
        )
        .mockResolvedValueOnce(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1b',
            },
          ]),
        )
        .mockResolvedValueOnce(transformFailure(scheduled401(['custom-1'])))
        .mockResolvedValueOnce(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-2',
            },
          ]),
        );

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
      expect(transformSpy.mock.calls[1][0].user).toEqual({id: 'user-1'});
      expect(transformSpy.mock.calls[2][0].auth?.raw).toBe('token-selected');
      expect(transformSpy.mock.calls[2][0].user).toEqual({id: 'user-1'});
      expect(transformSpy.mock.calls[3][0].auth?.raw).toBe('token-replacement');
      expect(transformSpy.mock.calls[3][0].user).toEqual({id: 'user-1'});
    });

    test('scheduled background retransform retries after transient query failure without disconnecting', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));
      using transformSpy = vi
        .spyOn(transformer!, 'transform')
        .mockResolvedValueOnce(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ]),
        )
        .mockResolvedValueOnce(transformFailure(scheduled500(['custom-1'])))
        .mockResolvedValueOnce(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ]),
        );

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
      await vi.waitFor(() =>
        expect(
          vs.connContextManager.getGroupState().retransformAt,
        ).toBeDefined(),
      );
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

  describe('background connection unavailable during pipeline sync', () => {
    let replicaDbFile: DbFile;
    let cvrDB: PostgresDB;
    let upstreamDb: PostgresDB;
    let stateChanges: Subscription<ReplicaState>;
    let vs: ViewSyncerService;
    let viewSyncerDone: Promise<void>;
    let connectWithQueueAndSource: (
      ctx: SyncContext,
      desiredQueriesPatch: UpQueriesPatch,
    ) => {
      queue: Queue<Downstream>;
      source: Source<ViewSyncerDownstream>;
    };
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
        connectWithQueueAndSource,
        customQueryTransformer,
        clearMocks,
      } = await setup(
        testDBs,
        'view_syncer_auth_maintenance_reconnect_race_test',
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

    test('defers pipeline init when background connection disconnects and replacement is not yet validated', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));
      using transformSpy = vi
        .spyOn(transformer!, 'transform')
        .mockResolvedValue(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ]),
        );

      const ctx1: SyncContext = {
        ...SYNC_CONTEXT,
        clientID: 'foo',
        wsID: 'ws1',
        auth: {type: 'opaque', raw: 'token-1'},
      };

      // 1. Client 1 connects and receives config poke.
      const {queue: client1, source: source1} = connectWithQueueAndSource(
        ctx1,
        [
          {
            op: 'put',
            hash: 'custom-1',
            name: 'named-query-1',
            args: ['thing'],
          },
        ],
      );
      await nextPoke(client1);

      // Client 1 has validated and is now the background connection.
      expect(validateSpy).toHaveBeenCalledTimes(1);
      expect(
        vs.connContextManager.getBackgroundConnectionContext(),
      ).toMatchObject({
        clientID: 'foo',
        wsID: 'ws1',
      });

      // 2. Client 1 disconnects.
      source1.cancel();
      // Disconnect cleanup closes the connection in connContextManager.
      await vi.waitFor(() => {
        expect(
          vs.connContextManager.getBackgroundConnectionContext(),
        ).toBeUndefined();
      });

      // 3. Replacement connection registers on connContextManager, but has NOT
      // validated yet (remains in provisional state).
      const ctx2: SyncContext = {
        ...SYNC_CONTEXT,
        clientID: 'foo',
        wsID: 'ws2',
        auth: {type: 'opaque', raw: 'token-2'},
      };
      const selector2 = {clientID: ctx2.clientID, wsID: ctx2.wsID};
      vs.connContextManager.registerConnection(
        selector2,
        {
          protocolVersion: ctx2.protocolVersion,
          clientID: ctx2.clientID,
          clientGroupID: serviceID,
          profileID: ctx2.profileID,
          baseCookie: ctx2.baseCookie,
          timestamp: Date.now(),
          lmID: 0,
          wsID: ctx2.wsID,
          debugPerf: false,
          auth: ctx2.auth?.raw,
          userID: ctx2.userID,
          initConnectionMsg: undefined,
          httpCookie: ctx2.httpCookie,
          origin: ctx2.origin,
        },
        ctx2.auth,
      );

      // Verify replacement is registered in provisional state without a background connection.
      expect(vs.connContextManager.getConnectionContext(selector2)?.state).toBe(
        'provisional',
      );
      expect(
        vs.connContextManager.getBackgroundConnectionContext(),
      ).toBeUndefined();

      // 4. Version-ready arrives while background connection is unavailable.
      // Before the fix, run() would call mustGetBackgroundConnectionContext()
      // and crash the entire ViewSyncer with "No validated connection is available for shared query work."
      stateChanges.push({state: 'version-ready'});

      // Give run() a moment to acquire the lock and execute.
      await sleep(50);

      // Verify pipeline init was deferred: custom queries were not transformed,
      // and the service is still running without crashing.
      expect(transformSpy).toHaveBeenCalledTimes(0);

      // 5. Replacement connection completes initConnection and validates.
      vs.connContextManager.initConnection(selector2, {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: 'custom-1',
            name: 'named-query-1',
            args: ['thing'],
          },
        ],
      });
      const source2 = vs.initConnection(selector2, [
        'initConnection',
        {
          desiredQueriesPatch: [
            {
              op: 'put',
              hash: 'custom-1',
              name: 'named-query-1',
              args: ['thing'],
            },
          ],
        },
      ]);
      const client2 = new Queue<Downstream>();
      void (async function () {
        try {
          for await (const {message} of source2) {
            client2.enqueue(message);
          }
        } catch (e) {
          client2.enqueueRejection(e);
        }
      })();

      // Wait for async initConnection validation and pipeline init to complete.
      await vi.waitFor(() => {
        expect(validateSpy).toHaveBeenCalledTimes(2);
        expect(
          vs.connContextManager.getBackgroundConnectionContext(),
        ).toMatchObject({
          clientID: 'foo',
          wsID: 'ws2',
        });
      });

      // Pipeline init and catchup ran immediately during initConnection,
      // delivering both the config patch and query rows in a single catchup poke.
      const pokes = await nextPoke(client2);
      expect(pokes).toBeDefined();

      expect(transformSpy).toHaveBeenCalledTimes(1);
      expect(transformSpy.mock.calls[0][0].auth?.raw).toBe('token-2');
    });

    test('does not crash when version-ready arrives after all clients disconnect', async () => {
      const transformer = customQueryTransformer;
      expect(transformer).toBeDefined();
      using validateSpy = vi
        .spyOn(transformer!, 'validate')
        .mockResolvedValue(validationSuccess('user-1'));
      using transformSpy = vi
        .spyOn(transformer!, 'transform')
        .mockResolvedValue(
          transformSuccess([
            {
              id: 'custom-1',
              transformedAst: ISSUES_QUERY,
              transformationHash: 'hash-1',
            },
          ]),
        );

      const ctx1: SyncContext = {
        ...SYNC_CONTEXT,
        clientID: 'foo',
        wsID: 'ws1',
        auth: {type: 'opaque', raw: 'token-1'},
      };

      // 1. Client connects and receives config poke.
      const {queue: client1, source: source1} = connectWithQueueAndSource(
        ctx1,
        [
          {
            op: 'put',
            hash: 'custom-1',
            name: 'named-query-1',
            args: ['thing'],
          },
        ],
      );
      await nextPoke(client1);
      expect(validateSpy).toHaveBeenCalledTimes(1);

      // 2. Client disconnects and NO replacement arrives.
      source1.cancel();
      await vi.waitFor(() => {
        expect(
          vs.connContextManager.getBackgroundConnectionContext(),
        ).toBeUndefined();
      });

      // 3. Version-ready arrives while no clients are connected.
      // Before the fix, run() would call mustGetBackgroundConnectionContext()
      // and crash with "No validated connection is available for shared query work."
      stateChanges.push({state: 'version-ready'});

      // Give run() a moment to process the version-ready.
      await sleep(50);

      // The service must not crash and custom query transforms must not run without credentials.
      expect(transformSpy).toHaveBeenCalledTimes(0);

      // Verify that viewSyncerDone is still pending (not crashed or failed).
      const timeout = sleep(50).then(() => 'still-running' as const);
      const status = await Promise.race([
        viewSyncerDone.then(() => 'stopped' as const),
        timeout,
      ]);
      expect(status).toBe('still-running');
    });
  });
});
