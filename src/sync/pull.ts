import type {LogContext} from '@rocicorp/logger';
import type * as dag from '../dag/mod';
import * as db from '../db/mod';
import type {ReadonlyJSONValue} from '../json';
import {
  assertPullResponse,
  Puller,
  PullerResult,
  PullError,
  PullResponse,
  PullResponseOK,
} from '../puller';
import {isClientStateNotFoundResponse} from './errors';
import {assertHTTPRequestInfo, HTTPRequestInfo} from '../http-request-info';
import {callJSRequest} from './js-request';
import {SYNC_HEAD_NAME} from './sync-head-name';
import * as patch from './patch';
import {toError} from '../to-error';
import * as btree from '../btree/mod';
import {BTreeRead} from '../btree/mod';
import {updateIndexes} from '../db/write';
import {emptyHash, Hash} from '../hash';
import type {Meta} from '../db/commit';
import type {InternalDiff} from '../btree/node.js';
import {allEntriesAsDiff} from '../btree/read.js';
import {
  toInternalValue,
  InternalValue,
  ToInternalValueReason,
  deepEqual,
} from '../internal-value.js';
import {assert} from '../asserts.js';

export const PULL_VERSION = 0;

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/server-pull).
 */
export type PullRequest<Cookie = ReadonlyJSONValue> = {
  profileID: string;
  clientID: string;
  cookie: Cookie;
  lastMutationID: number;
  pullVersion: number;
  // schema_version can optionally be used by the customer's app
  // to indicate to the data layer what format of Client View the
  // app understands.
  schemaVersion: string;
};

export type BeginPullRequest = {
  pullURL: string;
  pullAuth: string;
  schemaVersion: string;
  puller: Puller;
};

export type BeginPullResponse = {
  httpRequestInfo: HTTPRequestInfo;
  pullResponse?: PullResponse;
  syncHead: Hash;
};

export async function beginPull(
  profileID: string,
  clientID: string,
  beginPullReq: BeginPullRequest,
  puller: Puller,
  requestID: string,
  store: dag.Store,
  lc: LogContext,
  createSyncBranch = true,
): Promise<BeginPullResponse> {
  const {pullURL, pullAuth, schemaVersion} = beginPullReq;

  const baseSnapshot = await store.withRead(async dagRead => {
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (!mainHeadHash) {
      throw new Error('Internal no main head found');
    }
    return await db.baseSnapshot(mainHeadHash, dagRead);
  });

  const snapshotMetaParts = db.snapshotMetaParts(baseSnapshot);
  const baseCookie = snapshotMetaParts[1];

  const pullReq = {
    profileID,
    clientID,
    cookie: baseCookie,
    lastMutationID: baseSnapshot.mutationID,
    pullVersion: PULL_VERSION,
    schemaVersion,
  };
  lc.debug?.('Starting pull...');
  const pullStart = Date.now();
  const {response, httpRequestInfo} = await callPuller(
    puller,
    pullURL,
    pullReq,
    pullAuth,
    requestID,
  );

  lc.debug?.(
    `...Pull ${response ? 'complete' : 'failed'} in `,
    Date.now() - pullStart,
    'ms',
  );

  // If Puller did not get a pull response we still want to return the HTTP
  // request info to the JS SDK.
  if (!response) {
    return {
      httpRequestInfo,
      syncHead: emptyHash,
    };
  }

  if (!createSyncBranch || isClientStateNotFoundResponse(response)) {
    return {
      httpRequestInfo,
      pullResponse: response,
      syncHead: emptyHash,
    };
  }

  const syncHead = await handlePullResponse(lc, store, baseCookie, response);
  if (syncHead === null) {
    throw new Error('Overlapping sync JsLogInfo');
  }
  return {
    httpRequestInfo,
    pullResponse: response,
    syncHead,
  };
}

// Returns new sync head, or null if response did not apply due to mismatched cookie.
export async function handlePullResponse(
  lc: LogContext,
  store: dag.Store,
  expectedBaseCookie: InternalValue,
  response: PullResponseOK,
): Promise<Hash | null> {
  // It is possible that another sync completed while we were pulling. Ensure
  // that is not the case by re-checking the base snapshot.
  return await store.withWrite(async dagWrite => {
    const dagRead = dagWrite;
    const mainHead = await dagRead.getHead(db.DEFAULT_HEAD_NAME);

    if (mainHead === undefined) {
      throw new Error('Main head disappeared');
    }
    const baseSnapshot = await db.baseSnapshot(mainHead, dagRead);
    const [baseLastMutationID, baseCookie] = db.snapshotMetaParts(baseSnapshot);

    // TODO(MP) Here we are using whether the cookie has changes as a proxy for whether
    // the base snapshot changed, which is the check we used to do. I don't think this
    // is quite right. We need to firm up under what conditions we will/not accept an
    // update from the server: https://github.com/rocicorp/replicache/issues/713.
    if (!deepEqual(expectedBaseCookie, baseCookie)) {
      return null;
    }

    // If other entities (eg, other clients) are modifying the client view
    // the client view can change but the lastMutationID stays the same.
    // So be careful here to reject only a lesser lastMutationID.
    if (response.lastMutationID < baseLastMutationID) {
      throw new Error(
        `Received lastMutationID ${response.lastMutationID} is < than last snapshot lastMutationID ${baseLastMutationID}; ignoring client view`,
      );
    }

    const internalCookie = toInternalValue(
      response.cookie ?? null,
      ToInternalValueReason.CookieFromResponse,
    );

    // If there is no patch and the lmid and cookie don't change, it's a nop.
    // Otherwise, we will write a new commit, including for the case of just
    // a cookie change.
    if (
      response.patch.length === 0 &&
      response.lastMutationID === baseLastMutationID &&
      deepEqual(internalCookie, baseCookie)
    ) {
      return emptyHash;
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
    const lastIntegrated = chain.find(
      c => c.mutationID <= response.lastMutationID,
    );
    if (!lastIntegrated) {
      throw new Error('Internal invalid chain');
    }

    const dbWrite = await db.Write.newSnapshot(
      db.whenceHash(baseSnapshot.chunk.hash),
      response.lastMutationID,
      internalCookie,
      dagWrite,
      db.readIndexesForWrite(lastIntegrated, dagWrite),
    );

    await patch.apply(lc, dbWrite, response.patch);

    const lastIntegratedMap = new BTreeRead(dagRead, lastIntegrated.valueHash);

    for await (const change of dbWrite.map.diff(lastIntegratedMap)) {
      await updateIndexes(
        lc,
        dbWrite.indexes,
        change.key,
        () =>
          Promise.resolve(
            (change as {oldValue: InternalValue | undefined}).oldValue,
          ),
        (change as {newValue: InternalValue | undefined}).newValue,
      );
    }

    return await dbWrite.commit(SYNC_HEAD_NAME);
  });
}

/**
 * ReplayMutation is used in the RPC between EndPull so that we can replay
 * mutations on top of the current state. It is never exposed to the public.
 */
export type ReplayMutation = {
  id: number;
  name: string;
  args: InternalValue;
  original: Hash;
  timestamp: number;
};

// The diffs in different indexes. The key of the map is the index name.
// "" is used for the primary index.
export type DiffsMap = Map<string, InternalDiff>;

export type MaybeEndPullResult = {
  replayMutations?: ReplayMutation[];
  syncHead: Hash;
  diffs: DiffsMap;
};

export async function maybeEndPull(
  store: dag.Store,
  lc: LogContext,
  expectedSyncHead: Hash,
): Promise<MaybeEndPullResult> {
  // Ensure sync head is what the caller thinks it is.
  return await store.withWrite(async dagWrite => {
    const dagRead = dagWrite;
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
    const syncSnapshot = await db.baseSnapshot(syncHeadHash, dagRead);
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (mainHeadHash === undefined) {
      throw new Error('Missing main head');
    }
    const mainSnapshot = await db.baseSnapshot(mainHeadHash, dagRead);

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
    let pending = await db.localMutations(mainHeadHash, dagRead);
    const syncHead = await db.commitFromHash(syncHeadHash, dagRead);
    pending = pending.filter(c => c.mutationID > syncHead.mutationID);
    // pending() gave us the pending mutations in sync-head-first order whereas
    // caller wants them in the order to replay (lower mutation ids first).
    pending.reverse();

    // We return the keys that changed due to this pull. This is used by
    // subscriptions in the JS API when there are no more pending mutations.
    const diffs: DiffsMap = new Map();

    // Return replay commits if any.
    if (pending.length > 0) {
      const replayMutations: ReplayMutation[] = [];
      for (const c of pending) {
        let name: string;
        let args: InternalValue;
        let timestamp: number;
        if (c.isLocal()) {
          const lm = c.meta;
          name = lm.mutatorName;
          args = lm.mutatorArgsJSON;
          timestamp = lm.timestamp;
        } else {
          throw new Error('pending mutation is not local');
        }
        replayMutations.push({
          id: c.mutationID,
          name,
          args,
          original: c.chunk.hash,
          timestamp,
        });
      }
      return {
        syncHead: syncHeadHash,
        replayMutations,
        // The changed keys are not reported when further replays are
        // needed. The diffs will be reported at the end when there
        // are no more mutations to be replay and then it will be reported
        // relative to DEFAULT_HEAD_NAME.
        diffs,
      };
    }

    // TODO check invariants

    // Compute diffs (changed keys) for value map and index maps.
    const mainHead = await db.commitFromHash(mainHeadHash, dagRead);
    const mainHeadMap = new BTreeRead(dagRead, mainHead.valueHash);
    const syncHeadMap = new BTreeRead(dagRead, syncHead.valueHash);
    const valueDiff = await btree.diff(mainHeadMap, syncHeadMap);
    if (valueDiff.length > 0) {
      diffs.set('', valueDiff);
    }
    await addDiffsForIndexes(mainHead, syncHead, dagRead, diffs);

    // No mutations to replay so set the main head to the sync head and sync complete!
    await Promise.all([
      dagWrite.setHead(db.DEFAULT_HEAD_NAME, syncHeadHash),
      dagWrite.removeHead(SYNC_HEAD_NAME),
    ]);
    await dagWrite.commit();

    if (lc.debug) {
      const [oldLastMutationID, oldCookie] = db.snapshotMetaParts(mainSnapshot);
      const [newLastMutationID, newCookie] = db.snapshotMetaParts(syncSnapshot);
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
      diffs,
    };
  });
}

async function callPuller(
  puller: Puller,
  url: string,
  body: PullRequest<InternalValue>,
  auth: string,
  requestID: string,
): Promise<PullerResult> {
  try {
    const res = await callJSRequest(puller, url, body, auth, requestID);
    assertResult(res);
    return res;
  } catch (e) {
    throw new PullError(toError(e));
  }
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function assertResult(v: any): asserts v is PullerResult {
  if (typeof v !== 'object' || v === null) {
    throw new Error('Expected result to be an object');
  }

  if (v.response !== undefined) {
    assertPullResponse(v.response);
  }

  assertHTTPRequestInfo(v.httpRequestInfo);
}

async function addDiffsForIndexes(
  mainCommit: db.Commit<Meta>,
  syncCommit: db.Commit<Meta>,
  read: dag.Read,
  diffsMap: DiffsMap,
) {
  const oldIndexes = db.readIndexesForRead(mainCommit, read);
  const newIndexes = db.readIndexesForRead(syncCommit, read);

  for (const [oldIndexName, oldIndex] of oldIndexes) {
    const newIndex = newIndexes.get(oldIndexName);
    if (newIndex !== undefined) {
      assert(newIndex !== oldIndex);
      const diffs = await btree.diff(oldIndex.map, newIndex.map);
      newIndexes.delete(oldIndexName);
      if (diffs.length > 0) {
        diffsMap.set(oldIndexName, diffs);
      }
    } else {
      // old index name is not in the new indexes. All entries removed!
      const diffs = await allEntriesAsDiff(oldIndex.map, 'del');
      if (diffs.length > 0) {
        diffsMap.set(oldIndexName, diffs);
      }
    }
  }

  for (const [newIndexName, newIndex] of newIndexes) {
    // new index name is not in the old indexes. All keys added!
    const diffs = await allEntriesAsDiff(newIndex.map, 'add');
    if (diffs.length > 0) {
      diffsMap.set(newIndexName, diffs);
    }
  }
}
