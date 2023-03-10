import type {LogContext} from '@rocicorp/logger';
import type * as dag from '../dag/mod.js';
import * as db from '../db/mod.js';
import {
  deepEqual,
  FrozenJSONValue,
  ReadonlyJSONValue,
  deepFreeze,
} from '../json.js';
import type {
  PullResponseDD31,
  PullResponseOKDD31,
  PullResponseOKSDD,
  PullResponseSDD,
  Puller,
  PullerResultDD31,
  PullerResultSDD,
} from '../puller.js';
import {PullError} from './pull-error.js';
import type {HTTPRequestInfo} from '../http-request-info.js';
import {SYNC_HEAD_NAME} from './sync-head-name.js';
import * as patch from './patch.js';
import * as btree from '../btree/mod.js';
import {BTreeRead} from '../btree/mod.js';
import {updateIndexes} from '../db/write.js';
import {emptyHash, Hash} from '../hash.js';
import type {ClientGroupID, ClientID} from './ids.js';
import {addDiffsForIndexes, DiffComputationConfig, DiffsMap} from './diff.js';
import {assertSnapshotMetaDD31, commitIsLocalDD31} from '../db/commit.js';
import {compareCookies, Cookie} from '../cookies.js';
import {isErrorResponse} from '../error-responses.js';
import {toError} from '../to-error.js';
import {
  assertPullerResultDD31,
  assertPullerResultSDD,
} from '../get-default-puller.js';
import {withRead, withWrite} from '../with-transactions.js';

export const PULL_VERSION_SDD = 0;
export const PULL_VERSION_DD31 = 1;

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/reference/server-pull).
 */
export type PullRequest = PullRequestDD31 | PullRequestSDD;

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/reference/server-pull). This is the legacy version (V0) and it is
 * still used when recovering mutations from old clients.
 */
export type PullRequestSDD = {
  pullVersion: 0;
  // schemaVersion can optionally be used by the customer's app
  // to indicate to the data layer what format of Client View the
  // app understands.
  schemaVersion: string;
  profileID: string;
  cookie: ReadonlyJSONValue;

  clientID: ClientID;
  lastMutationID: number;
};

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/reference/server-pull).
 */
export type PullRequestDD31 = {
  pullVersion: 1;
  // schemaVersion can optionally be used by the customer's app
  // to indicate to the data layer what format of Client View the
  // app understands.
  schemaVersion: string;
  profileID: string;
  cookie: Cookie;

  clientGroupID: ClientGroupID;
};

export function isPullRequestDD31(pr: PullRequest): pr is PullRequestDD31 {
  return pr.pullVersion === PULL_VERSION_DD31;
}

export type BeginPullResponseDD31 = {
  httpRequestInfo: HTTPRequestInfo;
  pullResponse?: PullResponseDD31;
  syncHead: Hash;
};

export type BeginPullResponseSDD = {
  httpRequestInfo: HTTPRequestInfo;
  pullResponse?: PullResponseSDD;
  syncHead: Hash;
};

export async function beginPullSDD(
  profileID: string,
  clientID: ClientID,
  schemaVersion: string,
  puller: Puller,
  requestID: string,
  store: dag.Store,
  lc: LogContext,
  createSyncBranch = true,
): Promise<BeginPullResponseSDD> {
  const [lastMutationID, baseCookie] = await withRead(store, async dagRead => {
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (!mainHeadHash) {
      throw new Error('Internal no main head found');
    }
    const baseSnapshot = await db.baseSnapshotFromHash(mainHeadHash, dagRead);
    const baseSnapshotMeta = baseSnapshot.meta;
    const baseCookie = baseSnapshotMeta.cookieJSON;
    const lastMutationID = await baseSnapshot.getMutationID(clientID, dagRead);
    return [lastMutationID, baseCookie];
  });

  const pullReq: PullRequestSDD = {
    profileID,
    clientID,
    cookie: baseCookie,
    lastMutationID,
    pullVersion: PULL_VERSION_SDD,
    schemaVersion,
  };

  const {response, httpRequestInfo} = (await callPuller(
    lc,
    puller,
    pullReq,
    requestID,
  )) as PullerResultSDD;

  // If Puller did not get a pull response we still want to return the HTTP
  // request info to the JS SDK.
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

  const result = await handlePullResponseSDD(
    lc,
    store,
    baseCookie,
    response,
    clientID,
  );
  if (result.type === HandlePullResponseResultType.CookieMismatch) {
    throw new Error('Overlapping sync');
  }
  return {
    httpRequestInfo,
    pullResponse: response,
    syncHead:
      result.type === HandlePullResponseResultType.Applied
        ? result.syncHead
        : emptyHash,
  };
}

export async function beginPullDD31(
  profileID: string,
  clientID: ClientID,
  clientGroupID: ClientGroupID,
  schemaVersion: string,
  puller: Puller,
  requestID: string,
  store: dag.Store,
  lc: LogContext,
  createSyncBranch = true,
): Promise<BeginPullResponseDD31> {
  const baseCookie = await withRead(store, async dagRead => {
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (!mainHeadHash) {
      throw new Error('Internal no main head found');
    }
    const baseSnapshot = await db.baseSnapshotFromHash(mainHeadHash, dagRead);
    const baseSnapshotMeta = baseSnapshot.meta;
    assertSnapshotMetaDD31(baseSnapshotMeta);
    return baseSnapshotMeta.cookieJSON;
  });

  const pullReq: PullRequestDD31 = {
    profileID,
    clientGroupID,
    cookie: baseCookie,
    pullVersion: PULL_VERSION_DD31,
    schemaVersion,
  };

  const {response, httpRequestInfo} = (await callPuller(
    lc,
    puller,
    pullReq,
    requestID,
  )) as PullerResultDD31;

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

  const result = await handlePullResponseDD31(
    lc,
    store,
    baseCookie,
    response,
    clientID,
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
  pullReq: PullRequestDD31 | PullRequestSDD,
  requestID: string,
): Promise<PullerResultDD31 | PullerResultSDD> {
  lc.debug?.('Starting pull...');
  const pullStart = Date.now();
  try {
    const pullerResult = await puller(pullReq, requestID);
    lc.debug?.(
      `...Pull ${pullerResult.response ? 'complete' : 'failed'} in `,
      Date.now() - pullStart,
      'ms',
    );

    if (isPullRequestDD31(pullReq)) {
      assertPullerResultDD31(pullerResult);
    } else {
      assertPullerResultSDD(pullerResult);
    }

    return pullerResult;
  } catch (e) {
    throw new PullError(toError(e));
  }
}

// Returns new sync head, or null if response did not apply due to mismatched cookie.
export function handlePullResponseSDD(
  lc: LogContext,
  store: dag.Store,
  expectedBaseCookie: ReadonlyJSONValue,
  response: PullResponseOKSDD,
  clientID: ClientID,
): Promise<HandlePullResponseResult> {
  // It is possible that another sync completed while we were pulling. Ensure
  // that is not the case by re-checking the base snapshot.
  return withWrite(store, async dagWrite => {
    const dagRead = dagWrite;
    const mainHead = await dagRead.getHead(db.DEFAULT_HEAD_NAME);

    if (mainHead === undefined) {
      throw new Error('Main head disappeared');
    }
    const baseSnapshot = await db.baseSnapshotFromHash(mainHead, dagRead);
    const [baseLastMutationID, baseCookie] = db.snapshotMetaParts(
      baseSnapshot,
      clientID,
    );

    // TODO(MP) Here we are using whether the cookie has changes as a proxy for whether
    // the base snapshot changed, which is the check we used to do. I don't think this
    // is quite right. We need to firm up under what conditions we will/not accept an
    // update from the server: https://github.com/rocicorp/replicache/issues/713.
    if (!deepEqual(expectedBaseCookie, baseCookie)) {
      return {
        type: HandlePullResponseResultType.CookieMismatch,
      };
    }

    // If other entities (eg, other clients) are modifying the client view
    // the client view can change but the lastMutationID stays the same.
    // So be careful here to reject only a lesser lastMutationID.
    if (response.lastMutationID < baseLastMutationID) {
      throw new Error(
        badOrderMessage(
          `lastMutationID`,
          response.lastMutationID,
          baseLastMutationID,
        ),
      );
    }

    const frozenCookie = deepFreeze(response.cookie ?? null);

    // If there is no patch and the lmid and cookie don't change, it's a nop.
    // Otherwise, we will write a new commit, including for the case of just
    // a cookie change.
    if (
      response.patch.length === 0 &&
      response.lastMutationID === baseLastMutationID &&
      deepEqual(frozenCookie, baseCookie)
    ) {
      return {
        type: HandlePullResponseResultType.NoOp,
      };
    }

    // We are going to need to adjust the indexes. Imagine we have just pulled:
    //
    // S1 - M1 - main
    //    \ S2 - sync
    //
    // Let's say S2 says that it contains up to M1. Are we safe at this moment
    // to set main to S2?
    //
    // No, because the Replicache protocol does not require a snapshot
    // containing M1 to have the same data as the client computed for M1!
    //
    // We must diff the main map in M1 against the main map in S2 and see if it
    // contains any changes. Whatever changes it contains must be applied to
    // all indexes.
    //
    // We start with the index definitions in the last commit that was
    // integrated into the new snapshot.
    const chain = await db.commitChain(mainHead, dagRead);
    let lastIntegrated: db.Commit<db.Meta> | undefined;
    for (const commit of chain) {
      if (
        (await commit.getMutationID(clientID, dagRead)) <=
        response.lastMutationID
      ) {
        lastIntegrated = commit;
        break;
      }
    }

    if (!lastIntegrated) {
      throw new Error('Internal invalid chain');
    }

    const dbWrite = await db.newWriteSnapshotSDD(
      db.whenceHash(baseSnapshot.chunk.hash),
      response.lastMutationID,
      frozenCookie,
      dagWrite,
      db.readIndexesForWrite(lastIntegrated, dagWrite),
      clientID,
    );

    await patch.apply(lc, dbWrite, response.patch);

    const lastIntegratedMap = new BTreeRead(dagRead, lastIntegrated.valueHash);

    for await (const change of dbWrite.map.diff(lastIntegratedMap)) {
      await updateIndexes(
        lc,
        dbWrite.indexes,
        change.key,
        () =>
          Promise.resolve((change as {oldValue?: FrozenJSONValue}).oldValue),
        (change as {newValue?: FrozenJSONValue}).newValue,
      );
    }

    return {
      type: HandlePullResponseResultType.Applied,
      syncHead: await dbWrite.commit(SYNC_HEAD_NAME),
    };
  });
}

export enum HandlePullResponseResultType {
  Applied,
  NoOp,
  CookieMismatch,
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
  receivedValue: unknown,
  lastSnapshotValue: unknown,
) {
  return `Received ${name} ${receivedValue} is < than last snapshot ${name} ${lastSnapshotValue}; ignoring client view`;
}

export function handlePullResponseDD31(
  lc: LogContext,
  store: dag.Store,
  expectedBaseCookie: FrozenJSONValue,
  response: PullResponseOKDD31,
  clientID: ClientID,
): Promise<HandlePullResponseResult> {
  // It is possible that another sync completed while we were pulling. Ensure
  // that is not the case by re-checking the base snapshot.
  return withWrite(store, async dagWrite => {
    const dagRead = dagWrite;
    const mainHead = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (mainHead === undefined) {
      throw new Error('Main head disappeared');
    }
    const baseSnapshot = await db.baseSnapshotFromHash(mainHead, dagRead);
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
        'handlePullResponse: cookie mismatch, pull response is not applicable',
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
            lmidChange,
            lastMutationID,
          ),
        );
      }
    }

    const frozenResponseCookie = deepFreeze(response.cookie);
    if (compareCookies(frozenResponseCookie, baseCookie) < 0) {
      throw new Error(
        badOrderMessage('cookie', frozenResponseCookie, baseCookie),
      );
    }

    if (
      response.patch.length === 0 &&
      deepEqual(frozenResponseCookie, baseCookie) &&
      !anyMutationsToApply(
        response.lastMutationIDChanges,
        baseSnapshotMeta.lastMutationIDs,
      )
    ) {
      // If there is no patch and there are no lmid changes and cookie doesn't
      // change, it's a nop. Otherwise, something changed (maybe just the cookie)
      // and we will write a new commit.
      return {
        type: HandlePullResponseResultType.NoOp,
      };
    }

    const dbWrite = await db.newWriteSnapshotDD31(
      db.whenceHash(baseSnapshot.chunk.hash),
      {...baseSnapshotMeta.lastMutationIDs, ...response.lastMutationIDChanges},
      frozenResponseCookie,
      dagWrite,
      clientID,
    );

    await patch.apply(lc, dbWrite, response.patch);

    return {
      type: HandlePullResponseResultType.Applied,
      syncHead: await dbWrite.commit(SYNC_HEAD_NAME),
    };
  });
}

type MaybeEndPullResultBase<M extends db.Meta> = {
  replayMutations?: db.Commit<M>[];
  syncHead: Hash;
  diffs: DiffsMap;
};

export type MaybeEndPullResultSDD = MaybeEndPullResultBase<db.LocalMetaSDD>;

export function maybeEndPull<M extends db.LocalMeta>(
  store: dag.Store,
  lc: LogContext,
  expectedSyncHead: Hash,
  clientID: ClientID,
  diffConfig: DiffComputationConfig,
): Promise<{
  syncHead: Hash;
  replayMutations: db.Commit<M>[];
  diffs: DiffsMap;
}> {
  return withWrite(store, async dagWrite => {
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
    const syncSnapshot = await db.baseSnapshotFromHash(syncHeadHash, dagRead);
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (mainHeadHash === undefined) {
      throw new Error('Missing main head');
    }
    const mainSnapshot = await db.baseSnapshotFromHash(mainHeadHash, dagRead);

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
    const syncHead = await db.commitFromHash(syncHeadHash, dagRead);
    const pending: db.Commit<M>[] = [];
    const localMutations = await db.localMutations(mainHeadHash, dagRead);
    for (const commit of localMutations) {
      let cid = clientID;
      if (commitIsLocalDD31(commit)) {
        cid = commit.meta.clientID;
      }
      if (
        (await commit.getMutationID(cid, dagRead)) >
        (await syncHead.getMutationID(cid, dagRead))
      ) {
        // We know that the dag can only contain either LocalMetaSDD or LocalMetaDD31
        pending.push(commit as db.Commit<M>);
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
    const mainHead = await db.commitFromHash(mainHeadHash, dagRead);
    if (diffConfig.shouldComputeDiffs()) {
      const mainHeadMap = new BTreeRead(dagRead, mainHead.valueHash);
      const syncHeadMap = new BTreeRead(dagRead, syncHead.valueHash);
      const valueDiff = await btree.diff(mainHeadMap, syncHeadMap);
      diffsMap.set('', valueDiff);
      await addDiffsForIndexes(
        mainHead,
        syncHead,
        dagRead,
        diffsMap,
        diffConfig,
      );
    }

    // No mutations to replay so set the main head to the sync head and sync complete!
    await Promise.all([
      dagWrite.setHead(db.DEFAULT_HEAD_NAME, syncHeadHash),
      dagWrite.removeHead(SYNC_HEAD_NAME),
    ]);
    await dagWrite.commit();

    if (lc.debug) {
      const [oldLastMutationID, oldCookie] = db.snapshotMetaParts(
        mainSnapshot,
        clientID,
      );
      const [newLastMutationID, newCookie] = db.snapshotMetaParts(
        syncSnapshot,
        clientID,
      );
      lc.debug(
        `Successfully pulled new snapshot w/last_mutation_id:`,
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
        `, value_hash:`,
        syncHead.valueHash,
        `(prev:`,
        mainSnapshot.valueHash,
      );
    }

    return {
      syncHead: syncHeadHash,
      replayMutations: [],
      diffs: diffsMap,
    };
  });
}

function anyMutationsToApply(
  lastMutationIDChanges: Record<string, number>,
  lastMutationIDs: Record<string, number>,
) {
  for (const [clientID, lastMutationIDChange] of Object.entries(
    lastMutationIDChanges,
  )) {
    if (lastMutationIDChange !== lastMutationIDs[clientID]) {
      return true;
    }
  }
  return false;
}
