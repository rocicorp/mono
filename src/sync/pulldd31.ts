import type {LogContext} from '@rocicorp/logger';
import type * as dag from '../dag/mod';
import * as db from '../db/mod';
import {ReadonlyJSONValue, deepEqual as jsonDeepEqual} from '../json';
import {
  assertPullResponseDD31,
  isClientStateNotFoundResponse,
  PullerDD31,
  PullerResultDD31,
  PullError,
  PullResponseDD31,
  PullResponseOKDD31,
} from '../puller';
import {assertHTTPRequestInfo, HTTPRequestInfo} from '../http-request-info';
import {callJSRequest} from './js-request';
import {SYNC_HEAD_NAME} from './sync-head-name';
import * as patch from './patch';
import {toError} from '../to-error';
import * as btree from '../btree/mod';
import {BTreeRead} from '../btree/mod';
import {updateIndexes} from '../db/write';
import {emptyHash, Hash} from '../hash';
import {assertLocalMetaDD31, assertSnapshotMetaDD31, Meta} from '../db/commit';
import type {InternalDiff} from '../btree/node.js';
import {allEntriesAsDiff} from '../btree/read.js';
import {
  toInternalValue,
  InternalValue,
  ToInternalValueReason,
  deepEqual,
} from '../internal-value.js';
import {assert} from '../asserts.js';
import type {ClientID} from './client-id.js';
import {getClients} from '../persist/mod';
import type {BeginPullRequest} from './pull';

export const PULL_VERSION_DD31 = 1;

/**
 * The JSON value used as the body when doing a POST to the [pull
 * endpoint](/server-pull).
 */
export type PullRequestDD31<Cookie = ReadonlyJSONValue> = {
  profileID: string;
  clientID: string;
  cookie: Cookie;
  lastMutationIDs: Record<ClientID, number>;
  pullVersion: number;
  // schema_version can optionally be used by the customer's app
  // to indicate to the data layer what format of Client View the
  // app understands.
  schemaVersion: string;
};

export type BeginPullResponseDD31 = {
  httpRequestInfo: HTTPRequestInfo;
  pullResponse?: PullResponseDD31;
  syncHead: Hash;
};

export async function beginPull(
  profileID: string,
  clientID: string,
  beginPullReq: BeginPullRequest,
  puller: PullerDD31,
  requestID: string,
  store: dag.Store,
  lc: LogContext,
  createSyncBranch = true,
): Promise<BeginPullResponseDD31> {
  if (!DD31) {
    throw new Error();
  }
  const {pullURL, pullAuth, schemaVersion} = beginPullReq;

  const [lastMutationIDs, baseCookie] = await store.withRead(async dagRead => {
    const mainHeadHash = await dagRead.getHead(db.DEFAULT_HEAD_NAME);
    if (!mainHeadHash) {
      throw new Error('Internal no main head found');
    }
    const baseSnapshot = await db.baseSnapshot(mainHeadHash, dagRead);
    const baseCookie = baseSnapshot.meta.cookieJSON;
    const baseSnapshotMeta = baseSnapshot.meta;
    assertSnapshotMetaDD31(baseSnapshotMeta);
    const lastMutationIDs = {
      ...baseSnapshotMeta.lastMutationIDs,
    };
    const clients = await getClients(dagRead);
    for (const cID of clients.keys()) {
      if (lastMutationIDs[cID] === undefined) {
        lastMutationIDs[cID] = 0;
      }
    }
    const localMutations = await db.localMutations(mainHeadHash, dagRead);
    for (const localMutation of localMutations) {
      const localMeta = localMutation.meta;
      assertLocalMetaDD31(localMeta);
      const cID = localMeta.clientID;
      if (lastMutationIDs[cID] === undefined) {
        lastMutationIDs[cID] = 0;
      }
    }
    return [lastMutationIDs, baseCookie];
  });

  const pullReq = {
    profileID,
    clientID,
    cookie: baseCookie,
    lastMutationIDs,
    pullVersion: PULL_VERSION_DD31,
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

  const syncHead = await handlePullResponse(
    lc,
    store,
    baseCookie,
    lastMutationIDs,
    response,
    clientID,
  );
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
  requestLastMutationIDs: Record<ClientID, number>,
  response: PullResponseOKDD31,
  clientID: ClientID,
): Promise<Hash | null> {
  if (!DD31) {
    throw new Error();
  }
  // It is possible that another sync completed while we were pulling. Ensure
  // that is not the case by re-checking the base snapshot.
  return await store.withWrite(async dagWrite => {
    const dagRead = dagWrite;
    const mainHead = await dagRead.getHead(db.DEFAULT_HEAD_NAME);

    if (mainHead === undefined) {
      throw new Error('Main head disappeared');
    }
    const baseSnapshot = await db.baseSnapshot(mainHead, dagRead);
    const baseSnapshotMeta = baseSnapshot.meta;
    assertSnapshotMetaDD31(baseSnapshotMeta);
    const baseCookie = baseSnapshotMeta.cookieJSON;

    // TODO(MP) Here we are using whether the cookie has changes as a proxy for whether
    // the base snapshot changed, which is the check we used to do. I don't think this
    // is quite right. We need to firm up under what conditions we will/not accept an
    // update from the server: https://github.com/rocicorp/replicache/issues/713.
    // TODO(GREG) Should we check snapshot hashes instead of just cookie?
    if (!deepEqual(expectedBaseCookie, baseCookie)) {
      // TODO(GREG) debug log statement
      return null;
    }

    // If other entities (eg, other clients) are modifying the client view
    // the client view can change but the lastMutationID stays the same.
    // So be careful here to reject only a lesser lastMutationID.
    if (
      Object.keys(response.lastMutationIDs).length !==
      Object.keys(requestLastMutationIDs).length
    ) {
      throw new Error('');
    }
    for (const [clientID, lastMutationID] of Object.entries(
      requestLastMutationIDs,
    )) {
      const responseLastMutationID = response.lastMutationIDs[clientID];
      if (responseLastMutationID === undefined) {
        throw new Error('');
      }
      if (responseLastMutationID < lastMutationID) {
        throw new Error('');
      }
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
      jsonDeepEqual(requestLastMutationIDs, response.lastMutationIDs) &&
      deepEqual(internalCookie, baseCookie)
    ) {
      return emptyHash;
    }

    // Indexes need to be created for the new snapshot. To create, start
    // with the indexes of the the main head commit, then
    // diff the value map of the main head commit against the value map of
    // the new snapshot, and apply changes to the indexes.
    // Note: with this approach we won't lose any indexes, however
    // rebased mutations may see indexes which did not exist when
    // they were first executed.
    const mainHeadCommit = await db.commitFromHash(mainHead, dagRead);
    const dbWrite = await db.newWriteSnapshotDD31(
      db.whenceHash(baseSnapshot.chunk.hash),
      response.lastMutationIDs,
      internalCookie,
      dagWrite,
      db.readIndexesForWrite(mainHeadCommit, dagWrite),
      clientID,
    );
    await patch.apply(lc, dbWrite, response.patch);

    const mainHeadMap = new BTreeRead(dagRead, mainHeadCommit.valueHash);

    for await (const change of dbWrite.map.diff(mainHeadMap)) {
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

// The diffs in different indexes. The key of the map is the index name.
// "" is used for the primary index.
export type DiffsMap = Map<string, InternalDiff>;

export type MaybeEndPullResult = {
  replayMutations?: db.Commit<db.LocalMeta>[];
  syncHead: Hash;
  diffs: DiffsMap;
};

export async function maybeEndPull(
  store: dag.Store,
  lc: LogContext,
  expectedSyncHead: Hash,
  clientID: ClientID,
): Promise<MaybeEndPullResult> {
  if (!DD31) {
    throw new Error();
  }
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
    const syncHead = await db.commitFromHash(syncHeadHash, dagRead);
    const pending = [];
    const localMutations = await db.localMutations(mainHeadHash, dagRead);
    // maybe use localMutationsGreaterThan
    for (const commit of localMutations) {
      const {meta} = commit;
      assertLocalMetaDD31(meta);
      if (
        (await commit.getMutationID(meta.clientID, dagRead)) >
        (await syncHead.getMutationID(meta.clientID, dagRead))
      ) {
        pending.push(commit);
      }
    }
    // pending() gave us the pending mutations in sync-head-first order whereas
    // caller wants them in the order to replay (lower mutation ids first).
    pending.reverse();

    // We return the keys that changed due to this pull. This is used by
    // subscriptions in the JS API when there are no more pending mutations.
    const diffs: DiffsMap = new Map();

    // Return replay commits if any.
    if (pending.length > 0) {
      return {
        syncHead: syncHeadHash,
        replayMutations: pending,
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
      diffs,
    };
  });
}

async function callPuller(
  puller: PullerDD31,
  url: string,
  body: PullRequestDD31<InternalValue>,
  auth: string,
  requestID: string,
): Promise<PullerResultDD31> {
  try {
    const res = await callJSRequest(puller, url, body, auth, requestID);
    assertResult(res);
    return res;
  } catch (e) {
    throw new PullError(toError(e));
  }
}

function assertResult(v: unknown): asserts v is PullerResultDD31 {
  const result = v as PullerResultDD31;
  if (typeof result !== 'object' || result === null) {
    throw new Error('Expected result to be an object');
  }

  if (result.response !== undefined) {
    assertPullResponseDD31(result.response);
  }

  assertHTTPRequestInfo(result.httpRequestInfo);
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
