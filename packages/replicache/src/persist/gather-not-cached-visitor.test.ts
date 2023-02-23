import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod.js';
import {assertHash, makeNewFakeHashFunction} from '../hash.js';
import {ChainBuilder} from '../db/test-helpers.js';
import {GatherNotCachedVisitor} from './gather-not-cached-visitor.js';
import {MetaType} from '../db/commit.js';
import {withRead, withWrite} from '../with-transactions.js';

suite('GatherNotCachedVisitor', () => {
  test('when gatherSizeLimit not exceeded, if none cached gathers all, if all cached gathers none', async () => {
    const {perdag, memdag, pb, getSize, allChunksInVisitOrder} = await setup();

    const gatheredChunks = await withRead(perdag, async dagRead => {
      const visitor = new GatherNotCachedVisitor(
        dagRead,
        memdag,
        1000,
        getSize,
      );
      await visitor.visitCommit(pb.headHash);
      expect(
        Object.fromEntries(visitor.gatheredChunks.entries()),
      ).to.deep.equal(allChunksInVisitOrder);
      return visitor.gatheredChunks;
    });

    await withWrite(memdag, async dagWrite => {
      for (const {chunk, size} of gatheredChunks.values()) {
        await dagWrite.putChunk(chunk, size);
        await dagWrite.setHead('test', pb.headHash);
      }
      await dagWrite.commit();
    });

    await withRead(perdag, async dagRead => {
      const visitor = new GatherNotCachedVisitor(
        dagRead,
        memdag,
        1000,
        getSize,
      );
      await visitor.visitCommit(pb.headHash);
      expect(visitor.gatheredChunks).to.be.empty;
    });
  });

  test('gathers till gatherSizeLimit is exceeded with visit order: history order, values before indexes', async () => {
    const {perdag, memdag, pb, getSize, allChunksInVisitOrder} = await setup();

    await withRead(perdag, async dagRead => {
      const visitor = new GatherNotCachedVisitor(
        dagRead,
        memdag,
        // should gather 5, 4 * 10 < 45, so 5th is first exceeding limit
        45,
        getSize,
      );
      await visitor.visitCommit(pb.headHash);

      const allChunksInVisitOrderEntries = Object.entries(
        allChunksInVisitOrder,
      );
      const expectedChunks = Object.fromEntries(
        allChunksInVisitOrderEntries.slice(0, 5),
      );
      expect(
        Object.fromEntries(visitor.gatheredChunks.entries()),
      ).to.deep.equal(expectedChunks);
    });
  });

  test('stops gathering when cached chunks are reached', async () => {
    const {clientID, perdag, memdag, pb, getSize, allChunksInVisitOrder} =
      await setup();

    const gatheredChunks = await withRead(perdag, async dagRead => {
      const visitor = new GatherNotCachedVisitor(
        dagRead,
        memdag,
        1000,
        getSize,
      );
      await visitor.visitCommit(pb.headHash);
      expect(
        Object.fromEntries(visitor.gatheredChunks.entries()),
      ).to.deep.equal(allChunksInVisitOrder);
      return visitor.gatheredChunks;
    });

    await withWrite(memdag, async dagWrite => {
      for (const {chunk, size} of gatheredChunks.values()) {
        await dagWrite.putChunk(chunk, size);
        await dagWrite.setHead('test', pb.headHash);
      }
      await dagWrite.commit();
    });

    await pb.addLocal(clientID, [['localThree', {id: 'local3'}]]);

    await withRead(perdag, async dagRead => {
      const visitor = new GatherNotCachedVisitor(
        dagRead,
        memdag,
        1000,
        getSize,
      );
      await visitor.visitCommit(pb.headHash);
      expect(
        Object.fromEntries(visitor.gatheredChunks.entries()),
      ).to.deep.equal({
        face0000000040008000000000000000000000000014: {
          chunk: {
            hash: 'face0000000040008000000000000000000000000014',
            data: {
              meta: {
                type: MetaType.LocalDD31,
                basisHash: 'face0000000040008000000000000000000000000011',
                baseSnapshotHash:
                  'face0000000040008000000000000000000000000005',
                clientID: 'client-id',
                mutationID: 4,
                mutatorName: 'mutator_name_4',
                mutatorArgsJSON: [4],
                originalHash: null,
                timestamp: 42,
              },
              valueHash: 'face0000000040008000000000000000000000000012',
              indexes: [
                {
                  definition: {
                    name: 'testIndex',
                    keyPrefix: '',
                    jsonPointer: '/id',
                    allowEmpty: true,
                  },
                  valueHash: 'face0000000040008000000000000000000000000013',
                },
              ],
            },
            meta: [
              'face0000000040008000000000000000000000000012',
              'face0000000040008000000000000000000000000011',
              'face0000000040008000000000000000000000000013',
            ],
          },
          size: 10,
        },
        face0000000040008000000000000000000000000012: {
          chunk: {
            hash: 'face0000000040008000000000000000000000000012',
            data: [
              0,
              [
                ['localOne', {id: 'local1'}],
                ['localThree', {id: 'local3'}],
                ['localTwo', {id: 'local2'}],
                ['snapOne', {id: 'snap1'}],
                ['snapTwo', {id: 'snap2'}],
              ],
            ],
            meta: [],
          },
          size: 10,
        },
        face0000000040008000000000000000000000000013: {
          chunk: {
            hash: 'face0000000040008000000000000000000000000013',
            data: [
              0,
              [
                ['\u0000local1\u0000localOne', {id: 'local1'}],
                ['\u0000local2\u0000localTwo', {id: 'local2'}],
                ['\u0000local3\u0000localThree', {id: 'local3'}],
                ['\u0000snap1\u0000snapOne', {id: 'snap1'}],
                ['\u0000snap2\u0000snapTwo', {id: 'snap2'}],
              ],
            ],
            meta: [],
          },
          size: 10,
        },
      });
      return visitor.gatheredChunks;
    });
  });
});

async function setup() {
  const clientID = 'client-id';
  const hashFunction = makeNewFakeHashFunction();
  const getSize = () => 10;
  const perdag = new dag.TestStore(undefined, hashFunction);
  const memdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    hashFunction,
    assertHash,
    getSize,
  );

  const pb = new ChainBuilder(perdag);
  await pb.addGenesis(clientID, {
    testIndex: {
      jsonPointer: '/id',
      allowEmpty: true,
    },
  });
  await pb.addSnapshot(
    [
      ['snapOne', {id: 'snap1'}],
      ['snapTwo', {id: 'snap2'}],
    ],
    clientID,
  );
  await pb.addLocal(clientID, [['localOne', {id: 'local1'}]]);
  await pb.addLocal(clientID, [['localTwo', {id: 'local2'}]]);
  return {clientID, perdag, memdag, pb, getSize, allChunksInVisitOrder};
}

const allChunksInVisitOrder = {
  face0000000040008000000000000000000000000011: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000011',
      data: {
        meta: {
          type: MetaType.LocalDD31,
          basisHash: 'face0000000040008000000000000000000000000008',
          baseSnapshotHash: 'face0000000040008000000000000000000000000005',
          clientID: 'client-id',
          mutationID: 3,
          mutatorName: 'mutator_name_3',
          mutatorArgsJSON: [3],
          originalHash: null,
          timestamp: 42,
        },
        valueHash: 'face0000000040008000000000000000000000000009',
        indexes: [
          {
            definition: {
              name: 'testIndex',
              keyPrefix: '',
              jsonPointer: '/id',
              allowEmpty: true,
            },
            valueHash: 'face0000000040008000000000000000000000000010',
          },
        ],
      },
      meta: [
        'face0000000040008000000000000000000000000009',
        'face0000000040008000000000000000000000000008',
        'face0000000040008000000000000000000000000010',
      ],
    },
    size: 10,
  },
  face0000000040008000000000000000000000000009: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000009',
      data: [
        0,
        [
          ['localOne', {id: 'local1'}],
          ['localTwo', {id: 'local2'}],
          ['snapOne', {id: 'snap1'}],
          ['snapTwo', {id: 'snap2'}],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  face0000000040008000000000000000000000000010: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000010',
      data: [
        0,
        [
          ['\u0000local1\u0000localOne', {id: 'local1'}],
          ['\u0000local2\u0000localTwo', {id: 'local2'}],
          ['\u0000snap1\u0000snapOne', {id: 'snap1'}],
          ['\u0000snap2\u0000snapTwo', {id: 'snap2'}],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  face0000000040008000000000000000000000000008: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000008',
      data: {
        meta: {
          type: MetaType.LocalDD31,
          basisHash: 'face0000000040008000000000000000000000000005',
          baseSnapshotHash: 'face0000000040008000000000000000000000000005',
          clientID: 'client-id',
          mutationID: 2,
          mutatorName: 'mutator_name_2',
          mutatorArgsJSON: [2],
          originalHash: null,
          timestamp: 42,
        },
        valueHash: 'face0000000040008000000000000000000000000006',
        indexes: [
          {
            definition: {
              name: 'testIndex',
              keyPrefix: '',
              jsonPointer: '/id',
              allowEmpty: true,
            },
            valueHash: 'face0000000040008000000000000000000000000007',
          },
        ],
      },
      meta: [
        'face0000000040008000000000000000000000000006',
        'face0000000040008000000000000000000000000005',
        'face0000000040008000000000000000000000000007',
      ],
    },
    size: 10,
  },
  face0000000040008000000000000000000000000006: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000006',
      data: [
        0,
        [
          ['localOne', {id: 'local1'}],
          ['snapOne', {id: 'snap1'}],
          ['snapTwo', {id: 'snap2'}],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  face0000000040008000000000000000000000000007: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000007',
      data: [
        0,
        [
          ['\u0000local1\u0000localOne', {id: 'local1'}],
          ['\u0000snap1\u0000snapOne', {id: 'snap1'}],
          ['\u0000snap2\u0000snapTwo', {id: 'snap2'}],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  face0000000040008000000000000000000000000005: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000005',
      data: {
        meta: {
          type: MetaType.SnapshotDD31,
          basisHash: 'face0000000040008000000000000000000000000002',
          lastMutationIDs: {
            'client-id': 1,
          },
          cookieJSON: 'cookie_1',
        },
        valueHash: 'face0000000040008000000000000000000000000003',
        indexes: [
          {
            definition: {
              name: 'testIndex',
              keyPrefix: '',
              jsonPointer: '/id',
              allowEmpty: true,
            },
            valueHash: 'face0000000040008000000000000000000000000004',
          },
        ],
      },
      meta: [
        'face0000000040008000000000000000000000000003',
        'face0000000040008000000000000000000000000004',
      ],
    },
    size: 10,
  },
  face0000000040008000000000000000000000000003: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000003',
      data: [
        0,
        [
          ['snapOne', {id: 'snap1'}],
          ['snapTwo', {id: 'snap2'}],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  face0000000040008000000000000000000000000004: {
    chunk: {
      hash: 'face0000000040008000000000000000000000000004',
      data: [
        0,
        [
          ['\u0000snap1\u0000snapOne', {id: 'snap1'}],
          ['\u0000snap2\u0000snapTwo', {id: 'snap2'}],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
};
