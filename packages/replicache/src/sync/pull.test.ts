import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import {
  assertNotUndefined,
  assertObject,
  assertString,
} from 'shared/asserts.js';
import {asyncIterableToArray} from '../async-iterable-to-array.js';
import {BTreeRead} from '../btree/read.js';
import type {Cookie, FrozenCookie} from '../cookies.js';
import * as dag from '../dag/mod.js';
import {
  assertSnapshotCommitDD31,
  assertSnapshotCommitSDD,
  commitIsLocal,
} from '../db/commit.js';
import * as db from '../db/mod.js';
import {DEFAULT_HEAD_NAME} from '../db/mod.js';
import {ChainBuilder} from '../db/test-helpers.js';
import {
  isClientStateNotFoundResponse,
  isVersionNotSupportedResponse,
} from '../error-responses.js';
import {
  REPLICACHE_FORMAT_VERSION,
  REPLICACHE_FORMAT_VERSION_DD31,
  REPLICACHE_FORMAT_VERSION_SDD,
  ReplicacheFormatVersion,
} from '../format-version.js';
import {
  assertPullResponseV0,
  assertPullResponseV1,
} from '../get-default-puller.js';
import {assertHash, emptyHash} from '../hash.js';
import type {HTTPRequestInfo} from '../http-request-info.js';
import type {IndexDefinitions} from '../index-defs.js';
import {FrozenJSONValue, ReadonlyJSONValue, deepFreeze} from '../json.js';
import type {PatchOperation} from '../patch-operation.js';
import type {
  PullResponseOKV1,
  PullResponseV0,
  PullResponseV1,
  Puller,
  PullerResultV0,
  PullerResultV1,
} from '../puller.js';
import {stringCompare} from '../string-compare.js';
import {testSubscriptionsManagerOptions} from '../test-util.js';
import {withRead, withWrite} from '../with-transactions.js';
import type {DiffsMap} from './diff.js';
import {
  BeginPullResponseV0,
  BeginPullResponseV1,
  HandlePullResponseResultType,
  MaybeEndPullResultV0,
  PULL_VERSION_DD31,
  PULL_VERSION_SDD,
  PullRequestV0,
  PullRequestV1,
  beginPullV0,
  beginPullV1,
  handlePullResponseV1,
  isPullRequestV1,
  maybeEndPull,
} from './pull.js';
import {SYNC_HEAD_NAME} from './sync-head-name.js';

test('begin try pull SDD', async () => {
  const replicacheFormatVersion = REPLICACHE_FORMAT_VERSION_SDD;
  const clientID = 'test_client_id';
  const store = new dag.TestStore();
  const b = new ChainBuilder(store, undefined, REPLICACHE_FORMAT_VERSION_SDD);
  await b.addGenesis(clientID);
  const baseSnapshot = await b.addSnapshot([['foo', '"bar"']], clientID);
  await b.addIndexChange(clientID);
  const startingNumCommits = b.chain.length;
  const parts = db.snapshotMetaParts(baseSnapshot, clientID);

  const baseLastMutationID = parts[0];
  const baseCookie = deepFreeze(parts[1]);
  const baseValueMap = new Map([['foo', '"bar"']]);

  const requestID = 'requestID';
  const profileID = 'test_profile_id';
  const schemaVersion = 'schema_version';

  const goodHttpRequestInfo = {
    httpStatusCode: 200,
    errorMessage: '',
  };
  // The goodPullResp has a patch, a new cookie, and a new
  // lastMutationID. Tests can clone it and override those
  // fields they wish to change. This minimizes test changes required
  // when PullResponse changes.
  const newCookie = 'newCookie';
  const goodPullResp: PullResponseV0 = {
    cookie: newCookie,
    lastMutationID: 10,
    patch: [
      {op: 'clear'},
      {
        op: 'put',
        key: 'new',
        value: 'value',
      },
    ],
  };
  const goodPullRespValueMap = new Map([['new', 'value']]);

  type ExpCommit = {
    cookie: ReadonlyJSONValue;
    lastMutationID: number;
    valueMap: ReadonlyMap<string, ReadonlyJSONValue>;
    indexes: string[];
  };

  type Case = {
    name: string;
    createSyncBranch?: boolean;
    numPendingMutations: number;
    pullResult: PullResponseV0 | string;
    // BeginPull expectations.
    expNewSyncHead: ExpCommit | undefined;
    expBeginPullResult: BeginPullResponseV0 | string;
  };

  const expPullReq: PullRequestV0 = {
    profileID,
    clientID,
    cookie: baseCookie,
    lastMutationID: baseLastMutationID,
    pullVersion: PULL_VERSION_SDD,
    schemaVersion,
  };

  const cases: Case[] = [
    {
      name: '0 pending, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: goodPullResp,
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: goodPullResp.lastMutationID,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '0 pending, createSyncBranch false, pulls new state -> beginPull succeeds w/no syncHead',
      createSyncBranch: false,
      numPendingMutations: 0,
      pullResult: goodPullResp,
      expNewSyncHead: undefined,
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '1 pending, 0 mutations to replay, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 1,
      pullResult: {
        ...goodPullResp,
        lastMutationID: 2,
      },
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: 2,
        valueMap: goodPullRespValueMap,
        indexes: ['2', '4'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '1 pending, 1 mutations to replay, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 1,
      pullResult: {
        ...goodPullResp,
        lastMutationID: 1,
      },
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: 1,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '2 pending, 0 to replay, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 2,
      pullResult: goodPullResp,
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: goodPullResp.lastMutationID,
        valueMap: goodPullRespValueMap,
        indexes: ['2', '4', '6'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '2 pending, 1 to replay, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 2,
      pullResult: {
        ...goodPullResp,
        lastMutationID: 2,
      },
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: 2,
        valueMap: goodPullRespValueMap,
        indexes: ['2', '4'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    // The patch, lastMutationID, and cookie determine whether we write a new
    // Commit. Here we run through the different combinations.
    {
      name: 'no patch, same lmid, same cookie -> beginPull succeeds w/no syncHead',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationID: baseLastMutationID,
        cookie: baseCookie,
        patch: [],
      },
      expNewSyncHead: undefined,
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'new patch, same lmid, same cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationID: baseLastMutationID,
        cookie: baseCookie,
      },
      expNewSyncHead: {
        cookie: baseCookie,
        lastMutationID: baseLastMutationID,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'no patch, new lmid, same cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationID: baseLastMutationID + 1,
        cookie: baseCookie,
        patch: [],
      },
      expNewSyncHead: {
        cookie: baseCookie,
        lastMutationID: baseLastMutationID + 1,
        valueMap: baseValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'no patch, same lmid, new cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationID: baseLastMutationID,
        cookie: 'newCookie',
        patch: [],
      },
      expNewSyncHead: {
        cookie: 'newCookie',
        lastMutationID: baseLastMutationID,
        valueMap: baseValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'new patch, new lmid, same cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        cookie: baseCookie,
      },
      expNewSyncHead: {
        cookie: baseCookie,
        lastMutationID: goodPullResp.lastMutationID,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },

    {
      name: 'new patch, same lmid, new cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationID: baseLastMutationID,
      },
      expNewSyncHead: {
        cookie: goodPullResp.cookie ?? null,
        lastMutationID: baseLastMutationID,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'no patch, new lmid, new cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        patch: [],
      },
      expNewSyncHead: {
        cookie: goodPullResp.cookie ?? null,
        lastMutationID: goodPullResp.lastMutationID,
        valueMap: baseValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'new patch, new lmid, new cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
      },
      expNewSyncHead: {
        cookie: goodPullResp.cookie ?? null,
        lastMutationID: goodPullResp.lastMutationID,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'pulls new state w/lesser mutation id -> beginPull errors',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationID: 0,
      },
      expNewSyncHead: undefined,
      expBeginPullResult:
        'Received lastMutationID 0 is < than last snapshot lastMutationID 1; ignoring client view',
    },
    {
      name: 'pull 500s -> beginPull errors',
      numPendingMutations: 0,
      pullResult: 'FetchNotOk(500)',
      expNewSyncHead: undefined,
      expBeginPullResult: {
        httpRequestInfo: {
          errorMessage: 'Fetch not OK',
          httpStatusCode: 500,
        },
        syncHead: emptyHash,
      },
    },
  ];

  for (const c of cases) {
    // Reset state of the store.
    b.chain.length = startingNumCommits;
    await withWrite(store, async w => {
      await w.setHead(
        DEFAULT_HEAD_NAME,
        b.chain[b.chain.length - 1].chunk.hash,
      );
      await w.removeHead(SYNC_HEAD_NAME);
      await w.commit();
    });
    for (let i = 0; i < c.numPendingMutations; i++) {
      await b.addLocal(clientID);
      await b.addIndexChange(clientID);
    }

    // There was an index added after the snapshot, and one for each local commit.
    // Here we scan to ensure that we get values when scanning using one of the
    // indexes created. We do this because after calling beginPull we check that
    // the index no longer returns values, demonstrating that it was rebuilt.
    if (c.numPendingMutations > 0) {
      await withRead(store, async dagRead => {
        const read = await db.fromWhence(
          db.whenceHead(DEFAULT_HEAD_NAME),
          dagRead,
          replicacheFormatVersion,
        );
        let got = false;

        const indexMap = read.getMapForIndex('2');
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ of indexMap.scan('')) {
          got = true;
          break;
        }

        expect(got, c.name).to.be.true;
      });
    }

    // See explanation in FakePuller for why we do this dance with the pull_result.
    let pullResp;
    let pullErr;
    if (typeof c.pullResult === 'string') {
      pullResp = undefined;
      pullErr = c.pullResult;
    } else {
      pullResp = c.pullResult;
      pullErr = undefined;
    }
    const fakePuller = makeFakePuller({
      expPullReq,
      expRequestID: requestID,
      resp: pullResp,
      err: pullErr,
    });

    let result: BeginPullResponseV0 | string;
    try {
      result = await beginPullV0(
        profileID,
        clientID,
        schemaVersion,
        fakePuller,
        requestID,
        store,
        replicacheFormatVersion,
        new LogContext(),
        c.createSyncBranch,
      );
    } catch (e) {
      result = (e as Error).message;
      assertString(result);
    }

    await withRead(store, async read => {
      if (c.expNewSyncHead !== undefined) {
        const expSyncHead = c.expNewSyncHead;
        const syncHeadHash = await read.getHead(SYNC_HEAD_NAME);
        assertString(syncHeadHash);
        const chunk = await read.getChunk(syncHeadHash);
        assertNotUndefined(chunk);
        const syncHead = db.fromChunk(chunk);
        assertSnapshotCommitSDD(syncHead);
        const [gotLastMutationID, gotCookie] = db.snapshotMetaParts(
          syncHead,
          clientID,
        );
        expect(expSyncHead.lastMutationID).to.equal(gotLastMutationID);
        expect(expSyncHead.cookie).to.deep.equal(gotCookie);
        // Check the value is what's expected.
        const [, , bTreeRead] = await db.readCommitForBTreeRead(
          db.whenceHash(syncHead.chunk.hash),
          read,
          replicacheFormatVersion,
        );
        const gotValueMap = await asyncIterableToArray(bTreeRead.entries());
        gotValueMap.sort((a, b) => stringCompare(a[0], b[0]));
        const expValueMap = Array.from(expSyncHead.valueMap);
        expValueMap.sort((a, b) => stringCompare(a[0], b[0]));
        expect(expValueMap.length).to.equal(gotValueMap.length);

        // Check we have the expected index definitions.
        const indexes: string[] = syncHead.indexes.map(i => i.definition.name);
        expect(expSyncHead.indexes.length).to.equal(
          indexes.length,
          `${c.name}: expected indexes ${expSyncHead.indexes}, got ${indexes}`,
        );
        expSyncHead.indexes.forEach(
          i => expect(indexes.includes(i)).to.be.true,
        );

        // Check that we *don't* have old indexed values. The indexes should
        // have been rebuilt with a client view returned by the server that
        // does not include local= values. The check for len > 1 is because
        // the snapshot's index is not what we want; we want the first index
        // change's index ("2").
        if (expSyncHead.indexes.length > 1) {
          await withRead(store, async dagRead => {
            const read = await db.fromWhence(
              db.whenceHead(SYNC_HEAD_NAME),
              dagRead,
              replicacheFormatVersion,
            );
            const indexMap = read.getMapForIndex('2');
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            for await (const _ of indexMap.scan('')) {
              expect(false).to.be.true;
            }
          });

          assertObject(result);
          expect(syncHeadHash).to.equal(result.syncHead);
        }
      } else {
        const gotHead = await read.getHead(SYNC_HEAD_NAME);
        expect(gotHead).to.be.undefined;
        // When createSyncBranch is false or sync is a noop (empty patch,
        // same last mutation id, same cookie) we except BeginPull to succeed
        // but sync_head will be empty.
        if (typeof c.expBeginPullResult !== 'string') {
          assertObject(result);
          expect(result.syncHead).to.be.equal(emptyHash);
        }
      }

      expect(typeof result).to.equal(typeof c.expBeginPullResult);
      if (typeof result === 'object') {
        assertObject(c.expBeginPullResult);
        expect(result.httpRequestInfo).to.deep.equal(
          c.expBeginPullResult.httpRequestInfo,
        );
        if (typeof c.pullResult === 'object') {
          expect(result.pullResponse).to.deep.equal(c.pullResult);
        } else {
          expect(result.pullResponse).to.be.undefined;
        }
      } else {
        // use to_debug since some errors cannot be made PartialEq
        expect(result).to.equal(c.expBeginPullResult);
      }
    });
  }
});

test('begin try pull DD31', async () => {
  const replicacheFormatVersion = REPLICACHE_FORMAT_VERSION;
  const clientID = 'test_client_id';
  const clientGroupID = 'test_client_group_id';
  const baseCookie = 'cookie_1';
  const store = new dag.TestStore();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID, {
    '2': {prefix: 'local', jsonPointer: '', allowEmpty: false},
  });
  const baseSnapshot = await b.addSnapshot(
    [['foo', '"bar"']],
    clientID,
    baseCookie,
    undefined,
  );
  const startingNumCommits = b.chain.length;
  const parts = db.snapshotMetaParts(baseSnapshot, clientID);

  const baseLastMutationID = parts[0];
  const baseValueMap = new Map([['foo', '"bar"']]);

  const requestID = 'requestID';
  const profileID = 'test_profile_id';
  const schemaVersion = 'schema_version';

  const goodHttpRequestInfo = {
    httpStatusCode: 200,
    errorMessage: '',
  };
  // The goodPullResp has a patch, a new cookie, and a new
  // lastMutationID. Tests can clone it and override those
  // fields they wish to change. This minimizes test changes required
  // when PullResponse changes.
  const newCookie = 'cookie_2';
  const goodPullResp: PullResponseV1 = {
    cookie: newCookie,
    lastMutationIDChanges: {[clientID]: 10},
    patch: [
      {op: 'clear'},
      {
        op: 'put',
        key: 'new',
        value: 'value',
      },
    ],
  };
  const goodPullRespValueMap = new Map([['new', 'value']]);

  type ExpCommit = {
    cookie: ReadonlyJSONValue;
    lastMutationID: number;
    valueMap: ReadonlyMap<string, ReadonlyJSONValue>;
    indexes: string[];
  };

  type Case = {
    name: string;
    createSyncBranch?: boolean;
    numPendingMutations: number;
    pullResult: PullResponseV1 | string;
    // BeginPull expectations.
    expNewSyncHead: ExpCommit | undefined;
    expBeginPullResult: BeginPullResponseV1 | string;
  };

  const expPullReq: PullRequestV1 = {
    profileID,
    clientGroupID,
    cookie: baseCookie,
    pullVersion: PULL_VERSION_DD31,
    schemaVersion,
  };

  const cases: Case[] = [
    {
      name: '0 pending, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: goodPullResp,
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: goodPullResp.lastMutationIDChanges[clientID],
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '0 pending, createSyncBranch false, pulls new state -> beginPull succeeds w/no syncHead',
      createSyncBranch: false,
      numPendingMutations: 0,
      pullResult: goodPullResp,
      expNewSyncHead: undefined,
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '1 pending, 0 mutations to replay, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 1,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: 2},
      },
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: 2,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '1 pending, 1 mutations to replay, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 1,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: 1},
      },
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: 1,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '2 pending, 0 to replay, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 2,
      pullResult: goodPullResp,
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: goodPullResp.lastMutationIDChanges[clientID],
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: '2 pending, 1 to replay, pulls new state -> beginPull succeeds w/syncHead set',
      numPendingMutations: 2,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: 2},
      },
      expNewSyncHead: {
        cookie: newCookie,
        lastMutationID: 2,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    // The patch, lastMutationID, and cookie determine whether we write a new
    // Commit. Here we run through the different combinations.
    {
      name: 'no patch, same lmid, same cookie -> beginPull succeeds w/no syncHead',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: baseLastMutationID},
        cookie: baseCookie,
        patch: [],
      },
      expNewSyncHead: undefined,
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'new patch, same lmid, same cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: baseLastMutationID},
        cookie: baseCookie,
      },
      expNewSyncHead: {
        cookie: baseCookie,
        lastMutationID: baseLastMutationID,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'no patch, new lmid, same cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: baseLastMutationID + 1},
        cookie: baseCookie,
        patch: [],
      },
      expNewSyncHead: {
        cookie: baseCookie,
        lastMutationID: baseLastMutationID + 1,
        valueMap: baseValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'no patch, same lmid, new cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: baseLastMutationID},
        cookie: 'newCookie',
        patch: [],
      },
      expNewSyncHead: {
        cookie: 'newCookie',
        lastMutationID: baseLastMutationID,
        valueMap: baseValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'new patch, new lmid, same cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        cookie: baseCookie,
      },
      expNewSyncHead: {
        cookie: baseCookie,
        lastMutationID: goodPullResp.lastMutationIDChanges[clientID],
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },

    {
      name: 'new patch, same lmid, new cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: baseLastMutationID},
      },
      expNewSyncHead: {
        cookie: goodPullResp.cookie ?? null,
        lastMutationID: baseLastMutationID,
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'no patch, new lmid, new cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        patch: [],
      },
      expNewSyncHead: {
        cookie: goodPullResp.cookie ?? null,
        lastMutationID: goodPullResp.lastMutationIDChanges[clientID],
        valueMap: baseValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'new patch, new lmid, new cookie -> beginPull succeeds w/syncHead set',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
      },
      expNewSyncHead: {
        cookie: goodPullResp.cookie ?? null,
        lastMutationID: goodPullResp.lastMutationIDChanges[clientID],
        valueMap: goodPullRespValueMap,
        indexes: ['2'],
      },
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'pulls new state w/lesser mutation id -> beginPull errors',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        lastMutationIDChanges: {[clientID]: 0},
      },
      expNewSyncHead: undefined,
      expBeginPullResult:
        'Received test_client_id lastMutationID 0 is < than last snapshot test_client_id lastMutationID 1; ignoring client view',
    },
    {
      name: 'pulls new state w/lesser cookie -> beginPull errors',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        cookie: 'cookie_0',
      },
      expNewSyncHead: undefined,
      expBeginPullResult:
        'Received cookie cookie_0 is < than last snapshot cookie cookie_1; ignoring client view',
    },
    {
      name: 'pulls new state with identical client-lmid-changes in response (identical cookie and no patch)',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        cookie: 'cookie_1',
        patch: [],
        lastMutationIDChanges: {[clientID]: 1},
      },
      expNewSyncHead: undefined,
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'pulls new state with empty client-lmid-changes in response (identical cookie and no patch)',
      numPendingMutations: 0,
      pullResult: {
        ...goodPullResp,
        cookie: 'cookie_1',
        patch: [],
        lastMutationIDChanges: {},
      },
      expNewSyncHead: undefined,
      expBeginPullResult: {
        httpRequestInfo: goodHttpRequestInfo,
        syncHead: emptyHash,
      },
    },
    {
      name: 'pull 500s -> beginPull errors',
      numPendingMutations: 0,
      pullResult: 'FetchNotOk(500)',
      expNewSyncHead: undefined,
      expBeginPullResult: {
        httpRequestInfo: {
          errorMessage: 'Fetch not OK',
          httpStatusCode: 500,
        },
        syncHead: emptyHash,
      },
    },
  ];

  for (const c of cases) {
    // Reset state of the store.
    b.chain.length = startingNumCommits;
    await withWrite(store, async w => {
      await w.setHead(
        DEFAULT_HEAD_NAME,
        b.chain[b.chain.length - 1].chunk.hash,
      );
      await w.removeHead(SYNC_HEAD_NAME);
      await w.commit();
    });
    for (let i = 0; i < c.numPendingMutations; i++) {
      await b.addLocal(clientID);
    }

    // There was an index added after the snapshot, and one for each local commit.
    // Here we scan to ensure that we get values when scanning using one of the
    // indexes created. We do this because after calling beginPull we check that
    // the index no longer returns values, demonstrating that it was rebuilt.
    if (c.numPendingMutations > 0) {
      await withRead(store, async dagRead => {
        const read = await db.fromWhence(
          db.whenceHead(DEFAULT_HEAD_NAME),
          dagRead,
          replicacheFormatVersion,
        );
        let got = false;

        const indexMap = read.getMapForIndex('2');
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        for await (const _ of indexMap.scan('')) {
          got = true;
          break;
        }

        expect(got, c.name).to.be.true;
      });
    }

    // See explanation in FakePuller for why we do this dance with the pull_result.
    let pullResp: PullResponseV1 | undefined;
    let pullErr;
    if (typeof c.pullResult === 'string') {
      pullResp = undefined;
      pullErr = c.pullResult;
    } else {
      pullResp = c.pullResult;
      pullErr = undefined;
    }

    const fakePuller = makeFakePuller({
      expPullReq,
      expRequestID: requestID,
      resp: pullResp,
      err: pullErr,
    });

    let result: BeginPullResponseV1 | string;
    try {
      result = await beginPullV1(
        profileID,
        clientID,
        clientGroupID,
        schemaVersion,
        fakePuller,
        requestID,
        store,
        replicacheFormatVersion,
        new LogContext(),
        c.createSyncBranch,
      );
    } catch (e) {
      result = (e as Error).message;
      assertString(result);
    }

    await withRead(store, async read => {
      if (c.expNewSyncHead !== undefined) {
        const expSyncHead = c.expNewSyncHead;
        const syncHeadHash = await read.getHead(SYNC_HEAD_NAME);
        assertString(syncHeadHash);
        const chunk = await read.getChunk(syncHeadHash);
        assertNotUndefined(chunk);
        const syncHead = db.fromChunk(chunk);
        assertSnapshotCommitDD31(syncHead);
        const [gotLastMutationID, gotCookie] = db.snapshotMetaParts(
          syncHead,
          clientID,
        );
        expect(expSyncHead.lastMutationID).to.equal(gotLastMutationID);
        expect(expSyncHead.cookie).to.deep.equal(gotCookie);
        // Check the value is what's expected.
        const [, , bTreeRead] = await db.readCommitForBTreeRead(
          db.whenceHash(syncHead.chunk.hash),
          read,
          replicacheFormatVersion,
        );
        const gotValueMap = (
          await asyncIterableToArray(bTreeRead.entries())
        ).map(e => [e[0], e[1]] as const);
        gotValueMap.sort((a, b) => stringCompare(a[0], b[0]));
        const expValueMap = Array.from(expSyncHead.valueMap);
        expValueMap.sort((a, b) => stringCompare(a[0], b[0]));
        expect(expValueMap).to.deep.equal(gotValueMap);

        // Check we have the expected index definitions.
        const indexes: string[] = syncHead.indexes.map(i => i.definition.name);
        expect(expSyncHead.indexes).to.deep.equal(indexes);

        // Check that we *don't* have old indexed values. The indexes should
        // have been rebuilt with a client view returned by the server that
        // does not include local= values. The check for len > 1 is because
        // the snapshot's index is not what we want; we want the first index
        // change's index ("2").
        if (expSyncHead.indexes.length > 1) {
          await withRead(store, async dagRead => {
            const read = await db.fromWhence(
              db.whenceHead(SYNC_HEAD_NAME),
              dagRead,
              replicacheFormatVersion,
            );
            const indexMap = read.getMapForIndex('2');
            // eslint-disable-next-line @typescript-eslint/no-unused-vars
            for await (const _ of indexMap.scan('')) {
              expect(false).to.be.true;
            }
          });

          assertObject(result);
          expect(syncHeadHash).to.equal(result.syncHead);
        }
      } else {
        const gotHead = await read.getHead(SYNC_HEAD_NAME);
        expect(gotHead).to.be.undefined;
        // When createSyncBranch is false or sync is a noop (empty patch,
        // same last mutation id, same cookie) we except BeginPull to succeed
        // but sync_head will be empty.
        if (typeof c.expBeginPullResult !== 'string') {
          assertObject(result);
          expect(result.syncHead).to.be.equal(emptyHash);
        }
      }

      expect(typeof result).to.equal(typeof c.expBeginPullResult);
      if (typeof result === 'object') {
        assertObject(c.expBeginPullResult);
        expect(result.httpRequestInfo).to.deep.equal(
          c.expBeginPullResult.httpRequestInfo,
        );
        if (typeof c.pullResult === 'object') {
          expect(result.pullResponse).to.deep.equal(c.pullResult);
        } else {
          expect(result.pullResponse).to.be.undefined;
        }
      } else {
        // use to_debug since some errors cannot be made PartialEq
        expect(result).to.equal(c.expBeginPullResult);
      }
    });
  }
});

suite('maybe end try pull', () => {
  const t = async (replicacheFormatVersion: ReplicacheFormatVersion) => {
    const clientID = 'client-id';
    type Case = {
      name: string;
      numPending: number;
      numNeedingReplay: number;
      interveningSync: boolean;
      expReplayIDs: number[];
      expErr?: string | undefined;
      // The expected diffs as reported by the maybe end pull.
      expDiffs: DiffsMap;
    };
    const cases: Case[] = [
      {
        name: 'nothing pending',
        numPending: 0,
        numNeedingReplay: 0,
        interveningSync: false,
        expReplayIDs: [],
        expErr: undefined,
        expDiffs: new Map([['', [{op: 'add', key: 'key/0', newValue: '0'}]]]),
      },
      {
        name: '2 pending but nothing to replay',
        numPending: 2,
        numNeedingReplay: 0,
        interveningSync: false,
        expReplayIDs: [],
        expErr: undefined,
        expDiffs: new Map([
          [
            '',
            [
              {op: 'add', key: 'key/1', newValue: '1'},
              {op: 'del', key: 'local', oldValue: '2'},
            ],
          ],
        ]),
      },
      {
        name: '3 pending, 2 to replay',
        numPending: 3,
        numNeedingReplay: 2,
        interveningSync: false,
        expReplayIDs: [2, 3],
        expErr: undefined,
        // The changed keys are not reported when further replay is needed.
        expDiffs: new Map(),
      },
      {
        name: 'another sync landed during replay',
        numPending: 0,
        numNeedingReplay: 0,
        interveningSync: true,
        expReplayIDs: [],
        expErr: 'Overlapping syncs',
        expDiffs: new Map(),
      },
    ];

    for (const [i, c] of cases.entries()) {
      const store = new dag.TestStore();
      const lc = new LogContext();
      const b = new ChainBuilder(store);
      await b.addGenesis(clientID);
      // Add pending commits to the main chain.
      for (let j = 0; j < c.numPending; j++) {
        await b.addLocal(clientID);
      }
      let basisHash = await withWrite(store, async dagWrite => {
        await dagWrite.setHead(
          db.DEFAULT_HEAD_NAME,
          b.chain[b.chain.length - 1].chunk.hash,
        );

        // Add snapshot and replayed commits to the sync chain.
        const w =
          replicacheFormatVersion >= REPLICACHE_FORMAT_VERSION_DD31
            ? await db.newWriteSnapshotDD31(
                db.whenceHash(b.chain[0].chunk.hash),
                {[clientID]: 0},
                'sync_cookie',
                dagWrite,
                clientID,
                replicacheFormatVersion,
              )
            : await db.newWriteSnapshotSDD(
                db.whenceHash(b.chain[0].chunk.hash),
                0,
                'sync_cookie',
                dagWrite,
                db.readIndexesForWrite(
                  b.chain[0],
                  dagWrite,
                  replicacheFormatVersion,
                ),
                clientID,
                replicacheFormatVersion,
              );
        await w.put(lc, `key/${i}`, `${i}`);
        return w.commit(SYNC_HEAD_NAME);
      });

      if (c.interveningSync) {
        await b.addSnapshot(undefined, clientID);
      }

      for (let i = 0; i < c.numPending - c.numNeedingReplay; i++) {
        const chainIndex = i + 1; // chain[0] is genesis
        const original = b.chain[chainIndex];
        let mutatorName: string;
        let mutatorArgs: FrozenJSONValue;
        if (commitIsLocal(original)) {
          const lm = original.meta;
          mutatorName = lm.mutatorName;
          mutatorArgs = lm.mutatorArgsJSON;
        } else {
          throw new Error('impossible');
        }
        basisHash = await withWrite(store, async dagWrite => {
          const w = await db.newWriteLocal(
            db.whenceHash(basisHash),
            mutatorName,
            mutatorArgs,
            original.chunk.hash,
            dagWrite,
            original.meta.timestamp,
            clientID,
            replicacheFormatVersion,
          );
          return w.commit(SYNC_HEAD_NAME);
        });
      }
      const syncHead = basisHash;

      let result: MaybeEndPullResultV0 | string;
      try {
        result = await maybeEndPull(
          store,
          lc,
          syncHead,
          clientID,
          testSubscriptionsManagerOptions,
          replicacheFormatVersion,
        );
      } catch (e) {
        result = (e as Error).message;
      }

      if (c.expErr !== undefined) {
        const e = c.expErr;
        expect(result).to.equal(e);
      } else {
        assertObject(result);
        const resp = result;
        expect(syncHead).to.equal(resp.syncHead);
        expect(c.expReplayIDs.length).to.equal(
          resp.replayMutations?.length,
          `${c.name}: expected ${c.expReplayIDs}, got ${resp.replayMutations}`,
        );
        expect(Object.fromEntries(resp.diffs), c.name).to.deep.equal(
          Object.fromEntries(c.expDiffs),
        );

        for (let i = 0; i < c.expReplayIDs.length; i++) {
          const chainIdx = b.chain.length - c.numNeedingReplay + i;
          expect(c.expReplayIDs[i]).to.equal(
            resp.replayMutations?.[i].meta.mutationID,
          );
          const commit = b.chain[chainIdx];
          if (commitIsLocal(commit)) {
            expect(resp.replayMutations?.[i]).to.deep.equal(commit);
          } else {
            throw new Error('inconceivable');
          }
        }

        // Check if we set the main head like we should have.
        if (c.expReplayIDs.length === 0) {
          await withRead(store, async read => {
            expect(syncHead).to.equal(
              await read.getHead(db.DEFAULT_HEAD_NAME),
              c.name,
            );
            expect(await read.getHead(SYNC_HEAD_NAME)).to.be.undefined;
          });
        }
      }
    }
  };

  test('dd31', () => t(REPLICACHE_FORMAT_VERSION));
  test('sdd', () => t(REPLICACHE_FORMAT_VERSION_SDD));
});

type FakePullerArgs = {
  expPullReq: PullRequestV1 | PullRequestV0;
  expRequestID: string;
  resp?: PullResponseV1 | PullResponseV0 | undefined;
  err?: string | undefined;
};

function makeFakePuller(options: FakePullerArgs): Puller {
  return async (
    pullReq: PullRequestV1 | PullRequestV0,
    requestID: string,
    // eslint-disable-next-line require-await
  ): Promise<PullerResultV1 | PullerResultV0> => {
    expect(options.expPullReq).to.deep.equal(pullReq);
    expect(options.expRequestID).to.equal(requestID);

    let httpRequestInfo: HTTPRequestInfo;
    if (options.err !== undefined) {
      if (options.err === 'FetchNotOk(500)') {
        httpRequestInfo = {
          httpStatusCode: 500,
          errorMessage: 'Fetch not OK',
        };
      } else {
        throw new Error('not implemented');
      }
    } else {
      httpRequestInfo = {
        httpStatusCode: 200,
        errorMessage: '',
      };
    }

    const {resp} = options;

    if (resp === undefined) {
      return {httpRequestInfo};
    }

    if (
      isVersionNotSupportedResponse(resp) ||
      isClientStateNotFoundResponse(resp)
    ) {
      return {
        response: resp,
        httpRequestInfo,
      };
    }

    if (isPullRequestV1(options.expPullReq)) {
      assertPullResponseV1(resp);
      return {
        response: resp,
        httpRequestInfo,
      };
    }

    assertPullResponseV0(resp);
    return {
      response: resp,
      httpRequestInfo,
    };
  };
}

suite('changed keys', () => {
  const t = async (replicacheFormatVersion: ReplicacheFormatVersion) => {
    type IndexDef = {
      name: string;
      prefix: string;
      jsonPointer: string;
    };
    const t = async (
      baseMap: Map<string, string>,
      indexDef: IndexDef | undefined,
      patch: PatchOperation[],
      expectedDiffsMap: DiffsMap,
    ) => {
      const clientID = 'test_client_id';
      const clientGroupID = 'test_client_group__id';
      const store = new dag.TestStore();
      const lc = new LogContext();
      const b = new ChainBuilder(store, undefined, replicacheFormatVersion);

      if (indexDef) {
        const {name, prefix, jsonPointer} = indexDef;
        const indexDefinitions = {
          [name]: {
            jsonPointer,
            prefix,
            allowEmpty: false,
          },
        };

        if (replicacheFormatVersion >= REPLICACHE_FORMAT_VERSION_DD31) {
          await b.addGenesis(clientID, indexDefinitions);
          await b.addSnapshot([], clientID, undefined, undefined);
        } else {
          await b.addGenesis(clientID);
          await b.addIndexChange(clientID, name, {
            prefix,
            jsonPointer,
            allowEmpty: false,
          });
        }
      } else {
        await b.addGenesis(clientID);
      }

      const entries = [...baseMap];
      const baseSnapshot = await b.addSnapshot(entries, clientID);
      const parts = db.snapshotMetaParts(baseSnapshot, clientID);
      const baseLastMutationID = parts[0];
      const baseCookie = deepFreeze(parts[1]);

      const requestID = 'request_id';
      const profileID = 'test_profile_id';
      const schemaVersion = 'schema_version';

      const newCookie = 'new_cookie';

      const expPullReq: PullRequestV0 | PullRequestV1 =
        replicacheFormatVersion >= REPLICACHE_FORMAT_VERSION_DD31
          ? {
              profileID,
              clientGroupID,
              cookie: baseCookie as FrozenCookie,
              pullVersion: PULL_VERSION_DD31,
              schemaVersion,
            }
          : {
              profileID,
              clientID,
              cookie: baseCookie,
              lastMutationID: baseLastMutationID,
              pullVersion: PULL_VERSION_SDD,
              schemaVersion,
            };

      const pullResp: PullResponseV1 | PullResponseV0 =
        replicacheFormatVersion >= REPLICACHE_FORMAT_VERSION_DD31
          ? {
              cookie: newCookie,
              lastMutationIDChanges: {[clientID]: baseLastMutationID},
              patch,
            }
          : {
              cookie: newCookie,
              lastMutationID: baseLastMutationID,
              patch,
            };

      const puller = makeFakePuller({
        expPullReq,
        expRequestID: requestID,
        resp: pullResp,
        err: undefined,
      });

      const pullResult =
        replicacheFormatVersion >= REPLICACHE_FORMAT_VERSION_DD31
          ? await beginPullV1(
              profileID,
              clientID,
              clientGroupID,
              schemaVersion,
              puller,
              requestID,
              store,
              replicacheFormatVersion,
              new LogContext(),
            )
          : await beginPullV0(
              profileID,
              clientID,
              schemaVersion,
              puller,
              requestID,
              store,
              replicacheFormatVersion,
              new LogContext(),
            );

      const result = await maybeEndPull(
        store,
        lc,
        pullResult.syncHead,
        clientID,
        testSubscriptionsManagerOptions,
        replicacheFormatVersion,
      );
      expect(Object.fromEntries(result.diffs)).to.deep.equal(
        Object.fromEntries(expectedDiffsMap),
      );
    };

    await t(
      new Map(),
      undefined,
      [{op: 'put', key: 'key', value: 'value'}],
      new Map([
        [
          '',
          [
            {
              key: 'key',
              newValue: 'value',
              op: 'add',
            },
          ],
        ],
      ]),
    );

    await t(
      new Map([['foo', 'val']]),
      undefined,
      [{op: 'put', key: 'foo', value: 'new val'}],
      new Map([
        [
          '',
          [
            {
              op: 'change',
              key: 'foo',
              newValue: 'new val',
              oldValue: 'val',
            },
          ],
        ],
      ]),
    );

    await t(
      new Map([['a', '1']]),
      undefined,
      [{op: 'put', key: 'b', value: '2'}],
      new Map([['', [{op: 'add', key: 'b', newValue: '2'}]]]),
    );

    await t(
      new Map([['a', '1']]),
      undefined,
      [
        {op: 'put', key: 'b', value: '3'},
        {op: 'put', key: 'a', value: '2'},
      ],
      new Map([
        [
          '',
          [
            {op: 'change', key: 'a', oldValue: '1', newValue: '2'},
            {op: 'add', key: 'b', newValue: '3'},
          ],
        ],
      ]),
    );

    await t(
      new Map([
        ['a', '1'],
        ['b', '2'],
      ]),
      undefined,
      [{op: 'del', key: 'b'}],
      new Map([['', [{op: 'del', key: 'b', oldValue: '2'}]]]),
    );

    await t(
      new Map([
        ['a', '1'],
        ['b', '2'],
      ]),
      undefined,
      [{op: 'del', key: 'c'}],
      new Map(),
    );

    await t(
      new Map([
        ['a', '1'],
        ['b', '2'],
      ]),
      undefined,
      [{op: 'clear'}],
      new Map([
        [
          '',
          [
            {op: 'del', key: 'a', oldValue: '1'},
            {op: 'del', key: 'b', oldValue: '2'},
          ],
        ],
      ]),
    );

    await t(
      new Map([['a1', `{"id": "a-1", "x": 1}`]]),
      {
        name: 'i1',
        prefix: '',
        jsonPointer: '/id',
      },
      [{op: 'put', key: 'a2', value: {id: 'a-2', x: 2}}],
      new Map([
        [
          '',
          [
            {
              op: 'add',
              key: 'a2',
              newValue: deepFreeze({id: 'a-2', x: 2}),
            },
          ],
        ],
        [
          'i1',
          [
            {
              op: 'add',
              key: '\u{0}a-2\u{0}a2',
              newValue: deepFreeze({id: 'a-2', x: 2}),
            },
          ],
        ],
      ]),
    );

    await t(
      new Map(),
      {
        name: 'i1',
        prefix: '',
        jsonPointer: '/id',
      },
      [
        {op: 'put', key: 'a1', value: {id: 'a-1', x: 1}},
        {op: 'put', key: 'a2', value: {id: 'a-2', x: 2}},
      ],
      new Map([
        [
          '',
          [
            {
              op: 'add',
              key: 'a1',
              newValue: deepFreeze({id: 'a-1', x: 1}),
            },
            {
              op: 'add',
              key: 'a2',
              newValue: deepFreeze({id: 'a-2', x: 2}),
            },
          ],
        ],
        [
          'i1',
          [
            {
              op: 'add',
              key: '\u{0}a-1\u{0}a1',
              newValue: deepFreeze({id: 'a-1', x: 1}),
            },
            {
              op: 'add',
              key: '\u{0}a-2\u{0}a2',
              newValue: deepFreeze({id: 'a-2', x: 2}),
            },
          ],
        ],
      ]),
    );

    await t(
      new Map([['a1', `{"id": "a-1", "x": 1}`]]),
      {
        name: 'i1',
        prefix: '',
        jsonPointer: '/id',
      },
      [{op: 'put', key: 'a2', value: {id: 'a-2', x: 2}}],
      new Map([
        [
          '',
          [
            {
              op: 'add',
              key: 'a2',
              newValue: deepFreeze({id: 'a-2', x: 2}),
            },
          ],
        ],
        [
          'i1',
          [
            {
              op: 'add',
              key: '\u{0}a-2\u{0}a2',
              newValue: deepFreeze({id: 'a-2', x: 2}),
            },
          ],
        ],
      ]),
    );
  };

  test('dd31', () => t(REPLICACHE_FORMAT_VERSION));
  test('sdd', () => t(REPLICACHE_FORMAT_VERSION_SDD));
});

test('pull for client group with multiple client local changes', async () => {
  const replicacheFormatVersion = REPLICACHE_FORMAT_VERSION;
  const profileID = 'test-profile-id';
  const requestID = 'test-request-id';
  const clientID1 = 'test-client-id-1';
  const clientID2 = 'test-client-id-2';
  const clientGroupID = 'test-client-group-id';
  const schemaVersion = 'test-schema-version';

  const store = new dag.TestStore();
  const lc = new LogContext();

  const pullResponse = {
    cookie: 1,
    lastMutationIDChanges: {
      [clientID1]: 11,
      [clientID2]: 21,
    },
    patch: [],
  };

  const puller = makeFakePuller({
    expPullReq: {
      clientGroupID,
      cookie: 1,
      profileID,
      pullVersion: PULL_VERSION_DD31,
      schemaVersion,
    },
    expRequestID: requestID,
    resp: pullResponse,
  });

  const b = new ChainBuilder(store);
  await b.addGenesis(clientID1);
  await b.addSnapshot([], clientID1, 1, {
    [clientID1]: 10,
    [clientID2]: 20,
  });
  await b.addLocal(clientID1, []);
  await b.addLocal(clientID2, []);
  await b.addLocal(clientID1, []);
  await b.addLocal(clientID2, []);

  const response: BeginPullResponseV1 = await beginPullV1(
    profileID,
    clientID1,
    clientGroupID,
    schemaVersion,
    puller,
    requestID,
    store,
    replicacheFormatVersion,
    lc,
  );

  expect(response).to.deep.equal({
    httpRequestInfo: {
      errorMessage: '',
      httpStatusCode: 200,
    },
    pullResponse,
    syncHead: 'face0000000040008000000000000000000000000007',
  });
});

suite('beginPull DD31', () => {
  const replicacheFormatVersion = REPLICACHE_FORMAT_VERSION;
  const profileID = 'test-profile-id';
  const clientID1 = 'test-client-id-1';
  const clientGroupID1 = 'test-client-group-id-1';
  const requestID = 'test-request-id';
  const lc = new LogContext();

  test('no response should still return http status', async () => {
    const store = new dag.TestStore();

    const b = new ChainBuilder(store);
    await b.addGenesis(clientID1);

    const schemaVersion = 'test-schema-version';

    const options: FakePullerArgs = {
      expPullReq: {
        clientGroupID: clientGroupID1,
        cookie: null,
        profileID,
        pullVersion: PULL_VERSION_DD31,
        schemaVersion: 'test-schema-version',
      },
      expRequestID: requestID,
      resp: undefined,
    };
    const puller = makeFakePuller(options);

    const response = await beginPullV1(
      profileID,
      clientID1,
      clientGroupID1,
      schemaVersion,
      puller,
      requestID,
      store,
      replicacheFormatVersion,
      lc,
    );

    expect(response).to.deep.equal({
      httpRequestInfo: {
        errorMessage: '',
        httpStatusCode: 200,
      },
      syncHead: '00000000-0000-4000-8000-000000000000',
    });
  });
});

suite('handlePullResponseDD31', () => {
  const replicacheFormatVersion = REPLICACHE_FORMAT_VERSION;
  const clientID1 = 'test-client-id-1';
  const clientID2 = 'test-client-id-2';

  async function t({
    expectedBaseCookieJSON,
    responseCookie,
    expectedResultType,
    setupChain,
    responseLastMutationIDChanges = {},
    responsePatch = [],
    expectedMap,
    expectedIndex,
    expectedLastMutationIDs = responseLastMutationIDChanges,
    indexDefinitions,
  }: {
    expectedBaseCookieJSON: ReadonlyJSONValue;
    responseCookie: Cookie;
    expectedResultType: HandlePullResponseResultType;
    setupChain?: (b: ChainBuilder) => Promise<unknown>;
    responseLastMutationIDChanges?: {[clientID: string]: number};
    responsePatch?: PatchOperation[];
    expectedMap?: {[key: string]: ReadonlyJSONValue};
    expectedIndex?: [name: string, map: {[key: string]: ReadonlyJSONValue}];
    expectedLastMutationIDs?: {[clientID: string]: number};
    indexDefinitions?: IndexDefinitions | undefined;
  }) {
    const lc = new LogContext();
    const store = new dag.TestStore();

    const b = new ChainBuilder(store);
    await b.addGenesis(clientID1, indexDefinitions);
    await setupChain?.(b);

    const expectedBaseCookie = deepFreeze(expectedBaseCookieJSON);
    const response: PullResponseOKV1 = {
      cookie: responseCookie,
      lastMutationIDChanges: responseLastMutationIDChanges,
      patch: responsePatch,
    };

    const result = await handlePullResponseV1(
      lc,
      store,
      expectedBaseCookie,
      response,
      clientID1,
      replicacheFormatVersion,
    );

    expect(result.type).to.equal(expectedResultType);
    if (result.type === HandlePullResponseResultType.Applied) {
      assertHash(result.syncHead);

      await withRead(store, async dagRead => {
        const head = await db.commitFromHash(result.syncHead, dagRead);
        assertSnapshotCommitDD31(head);
        expect(head.chunk.data.meta.lastMutationIDs).to.deep.equal(
          expectedLastMutationIDs,
        );

        if (expectedMap) {
          const map = new BTreeRead(
            dagRead,
            replicacheFormatVersion,
            head.valueHash,
          );
          expect(
            Object.fromEntries(await asyncIterableToArray(map.entries())),
          ).deep.equal(expectedMap);
        }
        if (expectedIndex) {
          expect(head.indexes.length).to.equal(1);
          expect(head.indexes[0].definition.name).to.equal(expectedIndex[0]);
          const map = new BTreeRead(
            dagRead,
            replicacheFormatVersion,
            head.indexes[0].valueHash,
          );
          expect(
            Object.fromEntries(await asyncIterableToArray(map.entries())),
          ).deep.equal(expectedIndex[1]);
        }
      });
    }
  }

  test('If base cookie does not match we get emptyHash', async () => {
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.CookieMismatch,
    });
  });

  test('empty patch, no change in cookie, empty lmids', async () => {
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 1,
      expectedResultType: HandlePullResponseResultType.NoOp,
      setupChain: b => b.addSnapshot([], clientID1, 1, {}),
    });
  });

  test('empty patch, no change in cookie, non-empty lmids', async () => {
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 1,
      expectedResultType: HandlePullResponseResultType.NoOp,
      setupChain: b => b.addSnapshot([], clientID1, 1, {[clientID1]: 10}),
    });
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 1,
      expectedResultType: HandlePullResponseResultType.NoOp,
      setupChain: b => b.addSnapshot([], clientID1, 1, {[clientID2]: 20}),
    });
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 1,
      expectedResultType: HandlePullResponseResultType.NoOp,
      setupChain: b =>
        b.addSnapshot([], clientID1, 1, {
          [clientID1]: 10,
          [clientID2]: 20,
        }),
    });
  });

  test('change in cookie', async () => {
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: b => b.addSnapshot([], clientID1, 1, {[clientID1]: 10}),
      responseLastMutationIDChanges: {[clientID1]: 10},
    });
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: b => b.addSnapshot([], clientID1, 1, {[clientID2]: 20}),
      responseLastMutationIDChanges: {[clientID2]: 20},
    });
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: b =>
        b.addSnapshot([], clientID1, 1, {
          [clientID1]: 10,
          [clientID2]: 20,
        }),
      responseLastMutationIDChanges: {},
      expectedLastMutationIDs: {[clientID1]: 10, [clientID2]: 20},
    });

    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: b =>
        b.addSnapshot([], clientID1, 1, {
          [clientID1]: 10,
          [clientID2]: 20,
        }),
      responseLastMutationIDChanges: {[clientID1]: 11},
      expectedLastMutationIDs: {[clientID1]: 11, [clientID2]: 20},
    });

    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: b =>
        b.addSnapshot([], clientID1, 1, {
          [clientID1]: 10,
          [clientID2]: 20,
        }),
      responseLastMutationIDChanges: {[clientID2]: 21},
      expectedLastMutationIDs: {[clientID1]: 10, [clientID2]: 21},
    });

    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: b =>
        b.addSnapshot([], clientID1, 1, {
          [clientID1]: 10,
          [clientID2]: 20,
        }),
      responseLastMutationIDChanges: {[clientID1]: 11, [clientID2]: 21},
    });

    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: async b => {
        await b.addSnapshot([], clientID1, 1, {
          [clientID1]: 10,
          [clientID2]: 20,
        });
        await b.addLocal(clientID2, []);
      },
      responseLastMutationIDChanges: {[clientID1]: 11},
      expectedLastMutationIDs: {[clientID1]: 11, [clientID2]: 20},
    });
  });

  test('apply patch', async () => {
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: b => b.addSnapshot([], clientID1, 1, {[clientID1]: 10}),
      responseLastMutationIDChanges: {[clientID1]: 10},
      responsePatch: [
        {
          op: 'put',
          key: 'a',
          value: 0,
        },
      ],
      expectedMap: {a: 0},
    });

    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: async b => {
        await b.addSnapshot([], clientID1, 1, {[clientID1]: 10});
        await b.addLocal(clientID1, [['b', 1]]);
      },
      responseLastMutationIDChanges: {[clientID1]: 10},
      responsePatch: [
        {
          op: 'put',
          key: 'a',
          value: 0,
        },
      ],
      expectedMap: {a: 0},
    });
  });

  test('indexes do not include local commits', async () => {
    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: b => b.addSnapshot([], clientID1, 1, {[clientID1]: 10}),
      indexDefinitions: {
        i1: {
          prefix: '',
          jsonPointer: '/id',
        },
      },
      responseLastMutationIDChanges: {[clientID1]: 10},
      responsePatch: [
        {
          op: 'put',
          key: 'a',
          value: {id: 'aId', x: 2},
        },
      ],
      expectedMap: {a: {id: 'aId', x: 2}},
      expectedIndex: [
        'i1',
        {[db.encodeIndexKey(['aId', 'a'])]: {id: 'aId', x: 2}},
      ],
    });

    await t({
      expectedBaseCookieJSON: 1,
      responseCookie: 2,
      expectedResultType: HandlePullResponseResultType.Applied,
      setupChain: async b => {
        await b.addSnapshot([], clientID1, 1, {[clientID1]: 10});
        await b.addLocal(clientID1, [['b', {id: 'bId', x: 2}]]);
      },
      indexDefinitions: {
        i1: {
          prefix: '',
          jsonPointer: '/id',
        },
      },
      responseLastMutationIDChanges: {[clientID1]: 10},
      responsePatch: [
        {
          op: 'put',
          key: 'a',
          value: {id: 'aId', x: 2},
        },
      ],
      expectedMap: {a: {id: 'aId', x: 2}},
      expectedIndex: [
        'i1',
        {[db.encodeIndexKey(['aId', 'a'])]: {id: 'aId', x: 2}},
      ],
    });
  });
});
