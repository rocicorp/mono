import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {deepEqual} from '../../../shared/src/json.ts';
import {diff} from '../btree/diff.ts';
import {BTreeRead} from '../btree/read.ts';
import {compareCookies, type Cookie} from '../cookies.ts';
import type {Store} from '../dag/store.ts';
import type {Commit} from '../db/commit.ts';
import {
  assertSnapshotMetaDD31,
  baseSnapshotFromHash,
  commitFromHash,
  commitIsLocalDD31,
  DEFAULT_HEAD_NAME,
  type LocalMeta,
  localMutations as localMutations_1,
  snapshotMetaParts,
} from '../db/commit.ts';
import {newWriteSnapshotDD31} from '../db/write.ts';
import {isErrorResponse} from '../error-responses.ts';
import type * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze, type FrozenJSONValue} from '../frozen-json.ts';
import {assertPullerResultV1} from '../get-default-puller.ts';
import {emptyHash, type Hash} from '../hash.ts';
import type {HTTPRequestInfo} from '../http-request-info.ts';
import type {
  Puller,
  PullerResult,
  PullResponseOKV1Internal,
  PullResponseV1,
} from '../puller.ts';
import {ReportError} from '../report-error.ts';
import {toError} from '../to-error.ts';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.ts';
import {
  addDiffsForIndexes,
  type DiffComputationConfig,
  DiffsMap,
} from './diff.ts';
import * as HandlePullResponseResultType from './handle-pull-response-result-type-enum.ts';
import type {ClientGroupID, ClientID} from './ids.ts';
import * as patch from './patch.ts';
import {PullError} from './pull-error.ts';
import {SYNC_HEAD_NAME} from './sync-head-name.ts';

type FormatVersion = (typeof FormatVersion)[keyof typeof FormatVersion];

export const PULL_VERSION_SDD = 0;
export const PULL_VERSION_DD31 = 1;

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/reference/server-pull).
 */
export type PullRequest = PullRequestV1;

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/reference/server-pull).
 */
export type PullRequestV1 = {
  pullVersion: 1;
  // schemaVersion can optionally be used by the customer's app
  // to indicate to the data layer what format of Client View the
  // app understands.
  schemaVersion: string;
  profileID: string;
  cookie: Cookie;

  clientGroupID: ClientGroupID;
};

export function isPullRequestV1(pr: PullRequest): pr is PullRequestV1 {
  return pr.pullVersion === PULL_VERSION_DD31;
}

export type BeginPullResponseV1 = {
  httpRequestInfo: HTTPRequestInfo;
  pullResponse?: PullResponseV1;
  syncHead: Hash;
};

export async function beginPullV1(
  profileID: string,
  clientID: ClientID,
  clientGroupID: ClientGroupID,
  schemaVersion: string,
  puller: Puller,
  requestID: string,
  store: Store,
  formatVersion: FormatVersion,
  lc: LogContext,
  createSyncBranch = true,
): Promise<BeginPullResponseV1> {
  const baseCookie = await withRead(store, async dagRead => {
    const mainHeadHash = await dagRead.getHead(DEFAULT_HEAD_NAME);
    if (!mainHeadHash) {
      throw new Error('Internal no main head found');
    }
    const baseSnapshot = await baseSnapshotFromHash(mainHeadHash, dagRead);
    const baseSnapshotMeta = baseSnapshot.meta;
    assertSnapshotMetaDD31(baseSnapshotMeta);
    return baseSnapshotMeta.cookieJSON;
  });

  const pullReq: PullRequestV1 = {
    profileID,
    clientGroupID,
    cookie: baseCookie,
    pullVersion: PULL_VERSION_DD31,
    schemaVersion,
  };

  const {response, httpRequestInfo} = await callPuller(
    lc,
    puller,
    pullReq,
    requestID,
  );

  // If Puller did not get a pull response we still want to return the HTTP
  // request info.
  if (!response) {
    return {
      httpRequestInfo,
      syncHead: emptyHash,
    };
  }

  if (!createSyncBranch || isErrorResponse(response)) {
    return {
      httpRequestInfo,
      pullResponse: response,
      syncHead: emptyHash,
    };
  }

  const result = await handlePullResponseV1(
    lc,
    store,
    baseCookie,
    response,
    clientID,
    formatVersion,
  );

  return {
    httpRequestInfo,
    pullResponse: response,
    syncHead:
      result.type === HandlePullResponseResultType.Applied
        ? result.syncHead
        : emptyHash,
  };
}

async function callPuller(
  lc: LogContext,
  puller: Puller,
  pullReq: PullRequest,
  requestID: string,
): Promise<PullerResult> {
  lc.debug?.('Starting pull...');
  const pullStart = Date.now();
  let pullerResult: PullerResult;
  try {
    pullerResult = await puller(pullReq, requestID);
    lc.debug?.(
      `...Pull ${pullerResult.response ? 'complete' : 'failed'} in `,
      Date.now() - pullStart,
      'ms',
    );
  } catch (e) {
    throw new PullError(toError(e));
  }
  try {
    assertPullerResultV1(pullerResult);
    return pullerResult;
  } catch (e) {
    throw new ReportError('Invalid puller result', toError(e));
  }
}

type HandlePullResponseResult =
  | {
      type: HandlePullResponseResultType.Applied;
      syncHead: Hash;
    }
  | {
      type:
        | HandlePullResponseResultType.NoOp
        | HandlePullResponseResultType.CookieMismatch;
    };

function badOrderMessage(
  name: string,
  receivedValue: string,
  lastSnapshotValue: string,
) {
  return `Received ${name} ${receivedValue} is < than last snapshot ${name} ${lastSnapshotValue}; ignoring client view`;
}

export function handlePullResponseV1(
  lc: LogContext,
  store: Store,
  expectedBaseCookie: FrozenJSONValue,
  response: PullResponseOKV1Internal,
  clientID: ClientID,
  formatVersion: FormatVersion,
): Promise<HandlePullResponseResult> {
  // It is possible that another sync completed while we were pulling. Ensure
  // that is not the case by re-checking the base snapshot.
  return withWriteNoImplicitCommit(store, async dagWrite => {
    const dagRead = dagWrite;
    const mainHead = await dagRead.getHead(DEFAULT_HEAD_NAME);
    if (mainHead === undefined) {
      throw new Error('Main head disappeared');
    }
    const baseSnapshot = await baseSnapshotFromHash(mainHead, dagRead);
    const baseSnapshotMeta = baseSnapshot.meta;
    assertSnapshotMetaDD31(baseSnapshotMeta);
    const baseCookie = baseSnapshotMeta.cookieJSON;

    // TODO(MP) Here we are using whether the cookie has changed as a proxy for whether
    // the base snapshot changed, which is the check we used to do. I don't think this
    // is quite right. We need to firm up under what conditions we will/not accept an
    // update from the server: https://github.com/rocicorp/replicache/issues/713.
    // In DD31 this is expected to happen if a refresh occurs during a pull.
    if (!deepEqual(expectedBaseCookie, baseCookie)) {
      lc.debug?.(
        'handlePullResponse: cookie mismatch, response is not applicable',
      );
      return {
        type: HandlePullResponseResultType.CookieMismatch,
      };
    }

    // Check that the lastMutationIDs are not going backwards.
    for (const [clientID, lmidChange] of Object.entries(
      response.lastMutationIDChanges,
    )) {
      const lastMutationID = baseSnapshotMeta.lastMutationIDs[clientID];
      if (lastMutationID !== undefined && lmidChange < lastMutationID) {
        throw new Error(
          badOrderMessage(
            `${clientID} lastMutationID`,
            String(lmidChange),
            String(lastMutationID),
          ),
        );
      }
    }

    const frozenResponseCookie = deepFreeze(response.cookie);
    if (compareCookies(frozenResponseCookie, baseCookie) < 0) {
      throw new Error(
        badOrderMessage(
          'cookie',
          JSON.stringify(frozenResponseCookie),
          JSON.stringify(baseCookie),
        ),
      );
    }

    if (deepEqual(frozenResponseCookie, baseCookie)) {
      if (response.patch.length > 0) {
        lc.error?.(
          `handlePullResponse: cookie ${JSON.stringify(
            baseCookie,
          )} did not change, but patch is not empty`,
        );
      }
      if (Object.keys(response.lastMutationIDChanges).length > 0) {
        lc.error?.(
          `handlePullResponse: cookie ${JSON.stringify(
            baseCookie,
          )} did not change, but lastMutationIDChanges is not empty`,
        );
      }
      // If the cookie doesn't change, it's a nop.
      return {
        type: HandlePullResponseResultType.NoOp,
      };
    }

    const dbWrite = await newWriteSnapshotDD31(
      baseSnapshot.chunk.hash,
      {...baseSnapshotMeta.lastMutationIDs, ...response.lastMutationIDChanges},
      frozenResponseCookie,
      dagWrite,
      clientID,
      formatVersion,
    );

    await patch.apply(lc, dbWrite, response.patch);

    return {
      type: HandlePullResponseResultType.Applied,
      syncHead: await dbWrite.commit(SYNC_HEAD_NAME),
    };
  });
}

export function maybeEndPull<M extends LocalMeta>(
  store: Store,
  lc: LogContext,
  expectedSyncHead: Hash,
  clientID: ClientID,
  diffConfig: DiffComputationConfig,
  formatVersion: FormatVersion,
): Promise<{
  syncHead: Hash;
  mainHead: Hash;
  oldMainHead: Hash;
  replayMutations: Commit<M>[];
  diffs: DiffsMap;
}> {
  return withWriteNoImplicitCommit(store, async dagWrite => {
    const dagRead = dagWrite;
    // Ensure sync head is what the caller thinks it is.
    const syncHeadHash = await dagRead.getHead(SYNC_HEAD_NAME);
    if (syncHeadHash === undefined) {
      throw new Error('Missing sync head');
    }
    if (syncHeadHash !== expectedSyncHead) {
      lc.error?.(
        'maybeEndPull, Wrong sync head. Expecting:',
        expectedSyncHead,
        'got:',
        syncHeadHash,
      );
      throw new Error('Wrong sync head');
    }

    // Ensure another sync has not landed a new snapshot on the main chain.
    // TODO: In DD31, it is expected that a newer snapshot might have appeared
    // on the main chain. In that case, we just abort this pull.
    const syncSnapshot = await baseSnapshotFromHash(syncHeadHash, dagRead);
    const mainHeadHash = await dagRead.getHead(DEFAULT_HEAD_NAME);
    if (mainHeadHash === undefined) {
      throw new Error('Missing main head');
    }
    const mainSnapshot = await baseSnapshotFromHash(mainHeadHash, dagRead);

    const {meta} = syncSnapshot;
    const syncSnapshotBasis = meta.basisHash;
    if (syncSnapshot === null) {
      throw new Error('Sync snapshot with no basis');
    }
    if (syncSnapshotBasis !== mainSnapshot.chunk.hash) {
      throw new Error('Overlapping syncs');
    }

    // Collect pending commits from the main chain and determine which
    // of them if any need to be replayed.
    const syncHead = await commitFromHash(syncHeadHash, dagRead);
    const pending: Commit<M>[] = [];
    const localMutations = await localMutations_1(mainHeadHash, dagRead);
    for (const commit of localMutations) {
      let cid = clientID;
      assert(commitIsLocalDD31(commit));
      cid = commit.meta.clientID;

      if (
        (await commit.getMutationID(cid, dagRead)) >
        (await syncHead.getMutationID(cid, dagRead))
      ) {
        // We know that the dag can only contain either LocalMetaSDD or LocalMetaDD31
        pending.push(commit as Commit<M>);
      }
    }
    // pending() gave us the pending mutations in sync-head-first order whereas
    // caller wants them in the order to replay (lower mutation ids first).
    pending.reverse();

    // We return the keys that changed due to this pull. This is used by
    // subscriptions in the JS API when there are no more pending mutations.
    const diffsMap = new DiffsMap();

    // Return replay commits if any.
    if (pending.length > 0) {
      return {
        syncHead: syncHeadHash,
        oldMainHead: mainHeadHash,
        mainHead: mainHeadHash,
        replayMutations: pending,
        // The changed keys are not reported when further replays are
        // needed. The diffs will be reported at the end when there
        // are no more mutations to be replay and then it will be reported
        // relative to DEFAULT_HEAD_NAME.
        diffs: diffsMap,
      };
    }

    // TODO check invariants

    // Compute diffs (changed keys) for value map and index maps.
    const mainHead = await commitFromHash(mainHeadHash, dagRead);
    if (diffConfig.shouldComputeDiffs()) {
      const mainHeadMap = new BTreeRead(
        dagRead,
        formatVersion,
        mainHead.valueHash,
      );
      const syncHeadMap = new BTreeRead(
        dagRead,
        formatVersion,
        syncHead.valueHash,
      );
      const valueDiff = await diff(mainHeadMap, syncHeadMap);
      diffsMap.set('', valueDiff);
      await addDiffsForIndexes(
        mainHead,
        syncHead,
        dagRead,
        diffsMap,
        diffConfig,
        formatVersion,
      );
    }

    // No mutations to replay so set the main head to the sync head and sync complete!
    await Promise.all([
      dagWrite.setHead(DEFAULT_HEAD_NAME, syncHeadHash),
      dagWrite.removeHead(SYNC_HEAD_NAME),
    ]);
    await dagWrite.commit();
    // main head was set to sync head
    const newMainHeadHash = syncHeadHash;

    if (lc.debug) {
      const [oldLastMutationID, oldCookie] = snapshotMetaParts(
        mainSnapshot,
        clientID,
      );
      const [newLastMutationID, newCookie] = snapshotMetaParts(
        syncSnapshot,
        clientID,
      );
      lc.debug(
        `Successfully pulled new snapshot with lastMutationID:`,
        newLastMutationID,
        `(prev:`,
        oldLastMutationID,
        `), cookie: `,
        newCookie,
        `(prev:`,
        oldCookie,
        `), sync head hash:`,
        syncHeadHash,
        ', main head hash:',
        mainHeadHash,
        `, valueHash:`,
        syncHead.valueHash,
        `(prev:`,
        mainSnapshot.valueHash,
      );
    }

    return {
      syncHead: syncHeadHash,
      oldMainHead: mainHeadHash,
      mainHead: newMainHeadHash,
      replayMutations: [],
      diffs: diffsMap,
    };
  });
}
