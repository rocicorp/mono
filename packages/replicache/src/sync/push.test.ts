import {LogContext} from '@rocicorp/logger';
import {expect, test} from 'vitest';
import {TestStore} from '../dag/test-store.ts';
import {DEFAULT_HEAD_NAME} from '../db/commit.ts';
import {readFromDefaultHead} from '../db/read.ts';
import {ChainBuilder} from '../db/test-helpers.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze} from '../frozen-json.ts';
import type {Pusher, PusherResult} from '../pusher.ts';
import {withRead, withWrite} from '../with-transactions.ts';
import type {ClientGroupID} from './ids.ts';
import {
  PUSH_VERSION_ZERO,
  type PushRequest,
  type PushRequestV1,
  push,
} from './push.ts';
import {SYNC_HEAD_NAME} from './sync-head-name.ts';

type FakePusherArgs = {
  expPush: boolean;
  expPushReq?: PushRequest | undefined;
  expRequestID: string;
  error?: string | undefined;
};

function makeFakePusher(options: FakePusherArgs): Pusher {
  // eslint-disable-next-line require-await
  return async (pushReq, requestID): Promise<PusherResult> => {
    expect(options.expPush).to.be.true;

    if (options.expPushReq) {
      expect(options.expPushReq).to.deep.equal(pushReq);
      expect(options.expRequestID).to.equal(requestID);
    }

    if (options.error) {
      if (options.error === 'ClientStateNotFound') {
        return {
          response: {error: 'ClientStateNotFound'},
          httpRequestInfo: {
            httpStatusCode: 200,
            errorMessage: '',
          },
        };
      }
      if (options.error === 'FetchNotOk(500)') {
        return {
          httpRequestInfo: {
            httpStatusCode: 500,
            errorMessage: 'Fetch not OK',
          },
        };
      }
      throw new Error('not implemented');
    }

    return {
      httpRequestInfo: {
        httpStatusCode: 200,
        errorMessage: '',
      },
    };
  };
}

test('try push [DD31]', async () => {
  const formatVersion = FormatVersion.Latest;
  const clientGroupID = 'test_client_group_id';
  const clientID = 'test_client_id';
  const store = new TestStore();
  const lc = new LogContext();
  const b = new ChainBuilder(store);
  await b.addGenesis(clientID, {
    2: {prefix: 'local', jsonPointer: ''},
  });
  await b.addSnapshot([['foo', 'bar']], clientID);

  const startingNumCommits = b.chain.length;

  const requestID = 'request_id';
  const profileID = 'test_profile_id';

  // Push
  const pushSchemaVersion = 'pushSchemaVersion';

  type Case = {
    name: string;

    // Push expectations.
    numPendingMutations: number;
    expPushReq: PushRequestV1 | undefined;
    pushResult: undefined | 'ok' | {error: string};
    expPusherResult: PusherResult | undefined;
  };
  const cases: Case[] = [
    {
      name: '0 pending',
      numPendingMutations: 0,
      expPushReq: undefined,
      pushResult: undefined,
      expPusherResult: undefined,
    },
    {
      name: '1 pending',
      numPendingMutations: 1,
      expPushReq: {
        profileID,
        clientGroupID,
        mutations: [
          {
            clientID,
            id: 2,
            name: 'mutator_name_2',
            args: deepFreeze([2]),
            timestamp: 42,
          },
        ],
        pushVersion: PUSH_VERSION_ZERO,
        schemaVersion: pushSchemaVersion,
      },
      pushResult: 'ok',
      expPusherResult: {
        httpRequestInfo: {
          httpStatusCode: 200,
          errorMessage: '',
        },
      },
    },
    {
      name: '2 pending',
      numPendingMutations: 2,
      expPushReq: {
        profileID,
        clientGroupID,
        mutations: [
          // These mutations aren't actually added to the chain until the test
          // case runs, but we happen to know how they are created by the db
          // test helpers so we use that knowledge here.
          {
            clientID,
            id: 2,
            name: 'mutator_name_2',
            args: deepFreeze([2]),
            timestamp: 42,
          },
          {
            clientID,
            id: 3,
            name: 'mutator_name_3',
            args: deepFreeze([3]),
            timestamp: 42,
          },
        ],
        pushVersion: PUSH_VERSION_ZERO,
        schemaVersion: pushSchemaVersion,
      },
      pushResult: 'ok',
      expPusherResult: {
        httpRequestInfo: {
          httpStatusCode: 200,
          errorMessage: '',
        },
      },
    },
    {
      name: '2 mutations to push, push errors',
      numPendingMutations: 2,
      expPushReq: {
        profileID,
        clientGroupID: clientGroupID as ClientGroupID,
        mutations: [
          // These mutations aren't actually added to the chain until the test
          // case runs, but we happen to know how they are created by the db
          // test helpers so we use that knowledge here.
          {
            clientID,
            id: 2,
            name: 'mutator_name_2',
            args: deepFreeze([2]),
            timestamp: 42,
          },
          {
            clientID,
            id: 3,
            name: 'mutator_name_3',
            args: deepFreeze([3]),
            timestamp: 42,
          },
        ],
        pushVersion: PUSH_VERSION_ZERO,
        schemaVersion: pushSchemaVersion,
      },
      pushResult: {error: 'FetchNotOk(500)'},
      expPusherResult: {
        httpRequestInfo: {
          httpStatusCode: 500,
          errorMessage: 'Fetch not OK',
        },
      },
    },

    {
      name: '2 mutations to push, push errors',
      numPendingMutations: 2,
      expPushReq: {
        profileID,
        clientGroupID: clientGroupID as ClientGroupID,
        mutations: [
          // These mutations aren't actually added to the chain until the test
          // case runs, but we happen to know how they are created by the db
          // test helpers so we use that knowledge here.
          {
            clientID,
            id: 2,
            name: 'mutator_name_2',
            args: deepFreeze([2]),
            timestamp: 42,
          },
          {
            clientID,
            id: 3,
            name: 'mutator_name_3',
            args: deepFreeze([3]),
            timestamp: 42,
          },
        ],
        pushVersion: PUSH_VERSION_ZERO,
        schemaVersion: pushSchemaVersion,
      },
      pushResult: {error: 'FetchNotOk(500)'},
      expPusherResult: {
        httpRequestInfo: {
          httpStatusCode: 500,
          errorMessage: 'Fetch not OK',
        },
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
    });
    for (let i = 0; i < c.numPendingMutations; i++) {
      await b.addLocal(clientID);
    }

    // There was an index added after the snapshot, and one for each local
    // commit. Here we scan to ensure that we get values when scanning using one
    // of the indexes created. We do this because after calling begin_sync we
    // check that the index no longer returns values, demonstrating that it was
    // rebuilt.
    if (c.numPendingMutations > 0) {
      await withRead(store, async dagRead => {
        const read = await readFromDefaultHead(dagRead, formatVersion);
        let got = false;

        const indexMap = read.getMapForIndex('2');

        for await (const _ of indexMap.scan('')) {
          got = true;
          break;
        }
        expect(got).to.be.true;
      });
    }

    // See explanation in FakePusher for why we do this dance with the
    // push_result.
    const [expPush, pushErr] = (() => {
      switch (c.pushResult) {
        case undefined:
          return [false, undefined] as const;
        case 'ok':
          return [true, undefined] as const;
        default:
          return [true, c.pushResult.error] as const;
      }
    })();

    const pusher = makeFakePusher({
      expPush,
      expPushReq: c.expPushReq,
      expRequestID: requestID,
      error: pushErr,
    });

    const pusherResult = await push(
      requestID,
      store,
      lc,
      profileID,
      clientGroupID,
      clientID,
      pusher,
      pushSchemaVersion,
      PUSH_VERSION_ZERO,
    );

    expect(pusherResult).to.deep.equal(c.expPusherResult, `name: ${c.name}`);
  }
});
