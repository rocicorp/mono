import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import {DEFAULT_HEAD_NAME} from '../db/commit';
import {fromWhence, whenceHead} from '../db/read';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
} from '../db/test-helpers';
import {SYNC_HEAD_NAME} from './sync-head-name';
import {push, PushRequest, PUSH_VERSION} from './push';
import type {Pusher, PusherResult} from '../pusher';
import {toInternalValue, ToInternalValueReason} from '../internal-value.js';

type FakePusherArgs = {
  expPush: boolean;
  expPushReq?: PushRequest;
  expPushURL: string;
  expAuth: string;
  expRequestID: string;
  err?: string;
};

function makeFakePusher(options: FakePusherArgs): Pusher {
  return async (req: Request): Promise<PusherResult> => {
    expect(options.expPush).to.be.true;

    const pushReq = await req.json();

    if (options.expPushReq) {
      expect(options.expPushReq).to.deep.equal(pushReq);
      expect(new URL(options.expPushURL, location.href).toString()).to.equal(
        req.url,
      );
      expect(options.expAuth).to.equal(req.headers.get('Authorization'));
      expect(options.expRequestID).to.equal(
        req.headers.get('X-Replicache-RequestID'),
      );
    }

    if (options.err) {
      if (options.err === 'FetchNotOk(500)') {
        return {
          httpRequestInfo: {
            httpStatusCode: 500,
            errorMessage: 'Fetch not OK',
          },
          response: {},
        };
      } else {
        throw new Error('not implented');
      }
    }

    return {
      httpRequestInfo: {
        httpStatusCode: 200,
        errorMessage: '',
      },
      response: {},
    };
  };
}

test('try push', async () => {
  const store = new dag.TestStore();
  const lc = new LogContext();
  const chain: Chain = [];
  await addGenesis(chain, store);
  await addSnapshot(chain, store, [['foo', 'bar']]);
  // chain[2] is an index change
  await addIndexChange(chain, store);
  const startingNumCommits = chain.length;

  const requestID = 'request_id';
  const profileID = 'test_profile_id';
  const clientID = 'test_client_id';

  const auth = 'auth';

  // Push
  const pushURL = 'push_url';
  const pushSchemaVersion = 'pushSchemaVersion';

  type Case = {
    name: string;

    // Push expectations.
    numPendingMutations: number;
    expPushReq: PushRequest | undefined;
    pushResult: undefined | 'ok' | {error: string};
    expBatchPushInfo: PusherResult | undefined;
  };
  const cases: Case[] = [
    {
      name: '0 pending',
      numPendingMutations: 0,
      expPushReq: undefined,
      pushResult: undefined,
      expBatchPushInfo: undefined,
    },
    {
      name: '1 pending',
      numPendingMutations: 1,
      expPushReq: {
        profileID,
        clientID,
        mutations: [
          {
            id: 2,
            name: 'mutator_name_3',
            args: toInternalValue([3], ToInternalValueReason.Test),
            timestamp: 42,
          },
        ],
        pushVersion: PUSH_VERSION,
        schemaVersion: pushSchemaVersion,
      },
      pushResult: 'ok',
      expBatchPushInfo: {
        httpRequestInfo: {
          httpStatusCode: 200,
          errorMessage: '',
        },
        response: {},
      },
    },
    {
      name: '2 pending',
      numPendingMutations: 2,
      expPushReq: {
        profileID,
        clientID,
        mutations: [
          // These mutations aren't actually added to the chain until the test
          // case runs, but we happen to know how they are created by the db
          // test helpers so we use that knowledge here.
          {
            id: 2,
            name: 'mutator_name_3',
            args: toInternalValue([3], ToInternalValueReason.Test),
            timestamp: 42,
          },
          {
            id: 3,
            name: 'mutator_name_5',
            args: toInternalValue([5], ToInternalValueReason.Test),
            timestamp: 42,
          },
        ],
        pushVersion: PUSH_VERSION,
        schemaVersion: pushSchemaVersion,
      },
      pushResult: 'ok',
      expBatchPushInfo: {
        httpRequestInfo: {
          httpStatusCode: 200,
          errorMessage: '',
        },
        response: {},
      },
    },
    {
      name: '2 mutations to push, push errors',
      numPendingMutations: 2,
      expPushReq: {
        profileID,
        clientID,
        mutations: [
          // These mutations aren't actually added to the chain until the test
          // case runs, but we happen to know how they are created by the db
          // test helpers so we use that knowledge here.
          {
            id: 2,
            name: 'mutator_name_3',
            args: toInternalValue([3], ToInternalValueReason.Test),
            timestamp: 42,
          },
          {
            id: 3,
            name: 'mutator_name_5',
            args: toInternalValue([5], ToInternalValueReason.Test),
            timestamp: 42,
          },
        ],
        pushVersion: PUSH_VERSION,
        schemaVersion: pushSchemaVersion,
      },
      pushResult: {error: 'FetchNotOk(500)'},
      expBatchPushInfo: {
        httpRequestInfo: {
          httpStatusCode: 500,
          errorMessage: 'Fetch not OK',
        },
        response: {},
      },
    },
  ];

  for (const c of cases) {
    // Reset state of the store.
    chain.length = startingNumCommits;
    await store.withWrite(async w => {
      await w.setHead(DEFAULT_HEAD_NAME, chain[chain.length - 1].chunk.hash);
      await w.removeHead(SYNC_HEAD_NAME);
      await w.commit();
    });
    for (let i = 0; i < c.numPendingMutations; i++) {
      await addLocal(chain, store);
      await addIndexChange(chain, store);
    }

    // There was an index added after the snapshot, and one for each local
    // commit. Here we scan to ensure that we get values when scanning using one
    // of the indexes created. We do this because after calling begin_sync we
    // check that the index no longer returns values, demonstrating that it was
    // rebuilt.
    if (c.numPendingMutations > 0) {
      await store.withRead(async dagRead => {
        const read = await fromWhence(whenceHead(DEFAULT_HEAD_NAME), dagRead);
        let got = false;

        const indexMap = read.getMapForIndex('2');

        // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
      expPushURL: pushURL,
      expAuth: auth,
      expRequestID: requestID,
      err: pushErr,
    });

    const clientID = 'test_client_id';
    const batchPushInfo = await push(
      requestID,
      store,
      lc,
      profileID,
      clientID,
      pusher,
      pushURL,
      auth,
      pushSchemaVersion,
    );

    expect(batchPushInfo).to.deep.equal(c.expBatchPushInfo, `name: ${c.name}`);
  }
});
