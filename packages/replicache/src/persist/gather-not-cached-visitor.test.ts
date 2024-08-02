import {expect} from 'chai';
import {LazyStore} from '../dag/lazy-store.js';
import {TestStore} from '../dag/test-store.js';
import {MetaType} from '../db/commit.js';
import {ChainBuilder} from '../db/test-helpers.js';
import {assertHash, fakeHash, makeNewFakeHashFunction} from '../hash.js';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.js';
import {GatherNotCachedVisitor} from './gather-not-cached-visitor.js';

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
      await visitor.visit(pb.headHash);
      expect(
        Object.fromEntries(visitor.gatheredChunks.entries()),
      ).to.deep.equal(allChunksInVisitOrder);
      return visitor.gatheredChunks;
    });

    await withWriteNoImplicitCommit(memdag, async dagWrite => {
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
      await visitor.visit(pb.headHash);
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
      await visitor.visit(pb.headHash);

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
      await visitor.visit(pb.headHash);
      expect(
        Object.fromEntries(visitor.gatheredChunks.entries()),
      ).to.deep.equal(allChunksInVisitOrder);
      return visitor.gatheredChunks;
    });

    await withWriteNoImplicitCommit(memdag, async dagWrite => {
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
      await visitor.visit(pb.headHash);
      expect(
        Object.fromEntries(visitor.gatheredChunks.entries()),
      ).to.deep.equal({
        [fakeHash(14)]: {
          chunk: {
            hash: fakeHash(14),
            data: {
              meta: {
                type: MetaType.LocalDD31,
                basisHash: fakeHash(11),
                baseSnapshotHash: fakeHash(5),
                clientID: 'client-id',
                mutationID: 4,
                mutatorName: 'mutator_name_4',
                mutatorArgsJSON: [4],
                originalHash: null,
                timestamp: 42,
              },
              valueHash: fakeHash(12),
              indexes: [
                {
                  definition: {
                    name: 'testIndex',
                    keyPrefix: '',
                    jsonPointer: '/id',
                    allowEmpty: true,
                  },
                  valueHash: fakeHash(13),
                },
              ],
            },
            meta: [fakeHash(11), fakeHash(12), fakeHash(13)],
          },
          size: 10,
        },
        [fakeHash(12)]: {
          chunk: {
            hash: fakeHash(12),
            data: [
              0,
              [
                ['localOne', {id: 'local1'}, 48],
                ['localThree', {id: 'local3'}, 50],
                ['localTwo', {id: 'local2'}, 48],
                ['snapOne', {id: 'snap1'}, 46],
                ['snapTwo', {id: 'snap2'}, 46],
              ],
            ],
            meta: [],
          },
          size: 10,
        },
        [fakeHash(13)]: {
          chunk: {
            hash: fakeHash(13),
            data: [
              0,
              [
                ['\u0000local1\u0000localOne', {id: 'local1'}, 56],
                ['\u0000local2\u0000localTwo', {id: 'local2'}, 56],
                ['\u0000local3\u0000localThree', {id: 'local3'}, 58],
                ['\u0000snap1\u0000snapOne', {id: 'snap1'}, 53],
                ['\u0000snap2\u0000snapTwo', {id: 'snap2'}, 53],
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
  const perdag = new TestStore(undefined, hashFunction);
  const memdag = new LazyStore(
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
  [fakeHash(11)]: {
    chunk: {
      hash: fakeHash(11),
      data: {
        meta: {
          type: MetaType.LocalDD31,
          basisHash: fakeHash(8),
          baseSnapshotHash: fakeHash(5),
          clientID: 'client-id',
          mutationID: 3,
          mutatorName: 'mutator_name_3',
          mutatorArgsJSON: [3],
          originalHash: null,
          timestamp: 42,
        },
        valueHash: fakeHash(9),
        indexes: [
          {
            definition: {
              name: 'testIndex',
              keyPrefix: '',
              jsonPointer: '/id',
              allowEmpty: true,
            },
            valueHash: fakeHash(10),
          },
        ],
      },
      meta: [fakeHash(8), fakeHash(9), fakeHash(10)],
    },
    size: 10,
  },
  [fakeHash(9)]: {
    chunk: {
      hash: fakeHash(9),
      data: [
        0,
        [
          ['localOne', {id: 'local1'}, 48],
          ['localTwo', {id: 'local2'}, 48],
          ['snapOne', {id: 'snap1'}, 46],
          ['snapTwo', {id: 'snap2'}, 46],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  [fakeHash(8)]: {
    chunk: {
      hash: fakeHash(8),
      data: {
        meta: {
          type: MetaType.LocalDD31,
          basisHash: fakeHash(5),
          baseSnapshotHash: fakeHash(5),
          clientID: 'client-id',
          mutationID: 2,
          mutatorName: 'mutator_name_2',
          mutatorArgsJSON: [2],
          originalHash: null,
          timestamp: 42,
        },
        valueHash: fakeHash(6),
        indexes: [
          {
            definition: {
              name: 'testIndex',
              keyPrefix: '',
              jsonPointer: '/id',
              allowEmpty: true,
            },
            valueHash: fakeHash(7),
          },
        ],
      },
      meta: [fakeHash(5), fakeHash(6), fakeHash(7)],
    },
    size: 10,
  },
  [fakeHash(10)]: {
    chunk: {
      hash: fakeHash(10),
      data: [
        0,
        [
          ['\u0000local1\u0000localOne', {id: 'local1'}, 56],
          ['\u0000local2\u0000localTwo', {id: 'local2'}, 56],
          ['\u0000snap1\u0000snapOne', {id: 'snap1'}, 53],
          ['\u0000snap2\u0000snapTwo', {id: 'snap2'}, 53],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  [fakeHash(5)]: {
    chunk: {
      hash: fakeHash(5),
      data: {
        meta: {
          type: MetaType.SnapshotDD31,
          basisHash: fakeHash(2),
          lastMutationIDs: {
            'client-id': 1,
          },
          cookieJSON: 'cookie_1',
        },
        valueHash: fakeHash(3),
        indexes: [
          {
            definition: {
              name: 'testIndex',
              keyPrefix: '',
              jsonPointer: '/id',
              allowEmpty: true,
            },
            valueHash: fakeHash(4),
          },
        ],
      },
      meta: [fakeHash(3), fakeHash(4)],
    },
    size: 10,
  },
  [fakeHash(6)]: {
    chunk: {
      hash: fakeHash(6),
      data: [
        0,
        [
          ['localOne', {id: 'local1'}, 48],
          ['snapOne', {id: 'snap1'}, 46],
          ['snapTwo', {id: 'snap2'}, 46],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  [fakeHash(7)]: {
    chunk: {
      hash: fakeHash(7),
      data: [
        0,
        [
          ['\u0000local1\u0000localOne', {id: 'local1'}, 56],
          ['\u0000snap1\u0000snapOne', {id: 'snap1'}, 53],
          ['\u0000snap2\u0000snapTwo', {id: 'snap2'}, 53],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  [fakeHash(3)]: {
    chunk: {
      hash: fakeHash(3),
      data: [
        0,
        [
          ['snapOne', {id: 'snap1'}, 46],
          ['snapTwo', {id: 'snap2'}, 46],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
  [fakeHash(4)]: {
    chunk: {
      hash: fakeHash(4),
      data: [
        0,
        [
          ['\u0000snap1\u0000snapOne', {id: 'snap1'}, 53],
          ['\u0000snap2\u0000snapTwo', {id: 'snap2'}, 53],
        ],
      ],
      meta: [],
    },
    size: 10,
  },
};
