import {LogContext} from '@rocicorp/logger';
import {afterEach, expect, vi} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import {type PgTest, test} from '../../test/db.ts';
import {
  ALL_ISSUES_QUERY,
  ISSUES_QUERY,
  nextPoke,
  permissionsAll,
  setup,
} from './view-syncer-test-util.ts';
import {type SyncContext, TimeSliceTimer} from './view-syncer.ts';

const SYNC_CONTEXT: SyncContext = {
  clientID: 'foo',
  profileID: 'p0000g00000003203',
  wsID: 'ws1',
  baseCookie: null,
  protocolVersion: PROTOCOL_VERSION,
  httpCookie: undefined,
  origin: undefined,
  userID: 'bar',
  auth: undefined,
};

/** The same shape as {@link ISSUES_QUERY}: only its literal differs. */
const OTHER_ISSUES_QUERY: AST = {
  ...ISSUES_QUERY,
  where: {
    type: 'simple',
    left: {type: 'column', name: 'id'},
    op: 'IN',
    right: {type: 'literal', value: ['1', '2']},
  },
};

afterEach(() => {
  vi.restoreAllMocks();
});

test<PgTest>('logs slow hydrations redacted and throttled per query shape', async ({
  testDBs,
}) => {
  const logSink = new TestLogSink();
  const initial = await setup(testDBs, 'vs_slow_hydration', permissionsAll, {
    lc: new LogContext('debug', {}, logSink),
  });
  // Every hydration takes 150 ms, over the harness's 100 ms threshold.
  const stop = TimeSliceTimer.prototype.stop;
  vi.spyOn(TimeSliceTimer.prototype, 'stop').mockImplementation(
    function (this: TimeSliceTimer) {
      stop.call(this);
      return 150;
    },
  );
  try {
    const client = initial.connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'q1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'q2', ast: OTHER_ISSUES_QUERY},
      {op: 'put', hash: 'q3', ast: ALL_ISSUES_QUERY},
    ]);
    await nextPoke(client); // desired queries
    initial.stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // hydration

    const slowLogs = logSink.messages
      .filter(
        ([level, , args]) =>
          level === 'warn' && args[0] === 'Slow query materialization',
      )
      .map(([, , args]) => args[1] as Record<string, unknown>)
      .filter(log => ['q1', 'q2', 'q3'].includes(log.queryHash as string));

    // q1 and q2 share a shape, so only the first of them to hydrate is
    // logged.
    expect(slowLogs).toHaveLength(2);
    const issuesLog = slowLogs.find(log => log.queryHash !== 'q3');
    const allIssuesLog = slowLogs.find(log => log.queryHash === 'q3');
    expect(issuesLog).toEqual({
      zeroEvent: 'query-slow-hydration',
      clientGroupID: initial.vs.id,
      queryHash: expect.stringMatching(/^q[12]$/),
      transformationHash: expect.any(String),
      queryShape: expect.any(String),
      hydrationTimeMs: 150,
      hydrationRowCount: expect.any(Number),
      hydrationRowsRead: expect.any(Number),
      zql: `issues.where('id', 'IN', ?).orderBy('id', 'asc')`,
    });
    expect(allIssuesLog).toEqual({
      zeroEvent: 'query-slow-hydration',
      clientGroupID: initial.vs.id,
      queryHash: 'q3',
      transformationHash: expect.any(String),
      queryShape: expect.any(String),
      hydrationTimeMs: 150,
      hydrationRowCount: 5,
      hydrationRowsRead: 5,
      zql: `issues.orderBy('id', 'asc')`,
    });
    expect(issuesLog?.queryShape).not.toBe(allIssuesLog?.queryShape);
  } finally {
    initial.clearMocks();
    await initial.vs.stop();
    await initial.viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});
