import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod.js';
import * as db from '../db/mod.js';
import {assertHash, makeNewFakeHashFunction} from '../hash.js';
import {GatherMemoryOnlyVisitor} from './gather-mem-only-visitor.js';
import {ChainBuilder} from '../db/test-helpers.js';
import {MetaType} from '../db/commit.js';
import {TestLazyStore} from '../dag/test-lazy-store.js';

suite('dag with no memory-only hashes gathers nothing', () => {
  const t = async (dd31: boolean) => {
    const clientID = 'client-id';
    const hashFunction = makeNewFakeHashFunction();
    const perdag = new dag.TestStore(undefined, hashFunction);
    const memdag = new TestLazyStore(
      perdag,
      100 * 2 ** 20, // 100 MB,
      hashFunction,
      assertHash,
    );

    const pb = new ChainBuilder(perdag, undefined, dd31);
    await pb.addGenesis(clientID);
    await pb.addLocal(clientID);
    if (!dd31) {
      await pb.addIndexChange(clientID);
    }
    await pb.addLocal(clientID);

    await memdag.withRead(async dagRead => {
      for (const commit of pb.chain) {
        const visitor = new GatherMemoryOnlyVisitor(dagRead);
        await visitor.visitCommit(commit.chunk.hash);
        expect(visitor.gatheredChunks).to.be.empty;
      }
    });

    await pb.addSnapshot(undefined, clientID);

    await memdag.withRead(async dagRead => {
      const visitor = new GatherMemoryOnlyVisitor(dagRead);
      await visitor.visitCommit(pb.headHash);
      expect(visitor.gatheredChunks).to.be.empty;
    });
  };

  test('dd31', () => t(true));
  test('sdd', () => t(false));
});

suite('dag with only memory-only hashes gathers everything', () => {
  const t = async (dd31: boolean) => {
    const clientID = 'client-id';
    const hashFunction = makeNewFakeHashFunction();
    const perdag = new dag.TestStore(undefined, hashFunction);
    const memdag = new TestLazyStore(
      perdag,
      100 * 2 ** 20, // 100 MB,
      hashFunction,
      assertHash,
    );

    const mb = new ChainBuilder(memdag, undefined, dd31);

    const testGatheredChunks = async () => {
      await memdag.withRead(async dagRead => {
        const visitor = new GatherMemoryOnlyVisitor(dagRead);
        await visitor.visitCommit(mb.headHash);
        expect(memdag.getMemOnlyChunksSnapshot()).to.deep.equal(
          Object.fromEntries(visitor.gatheredChunks),
        );
      });
    };

    await mb.addGenesis(clientID);
    await mb.addLocal(clientID);
    await testGatheredChunks();

    await mb.addLocal(clientID);
    await testGatheredChunks();
    if (!dd31) {
      await mb.addIndexChange(clientID);
    }

    await mb.addSnapshot(undefined, clientID);
    await testGatheredChunks();
  };

  test('dd31', () => t(true));
  test('sdd', () => t(false));
});

suite(
  'dag with some persisted hashes and some memory-only hashes on top',
  () => {
    const t = async (dd31: boolean) => {
      const clientID = 'client-id';
      const hashFunction = makeNewFakeHashFunction();
      const perdag = new dag.TestStore(undefined, hashFunction);
      const memdag = new dag.LazyStore(
        perdag,
        100 * 2 ** 20, // 100 MB,
        hashFunction,
        assertHash,
      );

      const pb = new ChainBuilder(perdag, undefined, dd31);
      const mb = new ChainBuilder(memdag, undefined, dd31);

      await pb.addGenesis(clientID);
      await pb.addLocal(clientID);

      await memdag.withWrite(async memdagWrite => {
        await memdagWrite.setHead(db.DEFAULT_HEAD_NAME, pb.headHash);
        await memdagWrite.commit();
      });
      mb.chain = pb.chain.slice();
      await mb.addLocal(clientID);

      await memdag.withRead(async dagRead => {
        const visitor = new GatherMemoryOnlyVisitor(dagRead);
        await visitor.visitCommit(mb.headHash);
        const metaBase = {
          basisHash: 'face0000000040008000000000000000' + '' + '000000000003',
          mutationID: 2,
          mutatorArgsJSON: [2],
          mutatorName: 'mutator_name_2',
          originalHash: null,
          timestamp: 42,
        };
        const meta = dd31
          ? {
              type: MetaType.LocalDD31,
              ...metaBase,
              baseSnapshotHash:
                'face0000000040008000000000000000' + '' + '000000000001',
              clientID,
            }
          : {type: MetaType.LocalSDD, ...metaBase};
        expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal({
          ['face0000000040008000000000000000' + '' + '000000000004']: {
            data: [0, [['local', '2']]],
            hash: 'face0000000040008000000000000000' + '' + '000000000004',
            meta: [],
          },
          ['face0000000040008000000000000000' + '' + '000000000005']: {
            data: {
              indexes: [],
              meta,
              valueHash:
                'face0000000040008000000000000000' + '' + '000000000004',
            },
            hash: 'face0000000040008000000000000000' + '' + '000000000005',
            meta: [
              'face0000000040008000000000000000' + '' + '000000000004',
              'face0000000040008000000000000000' + '' + '000000000003',
            ],
          },
        });
      });
    };
    test('dd31', () => t(true));
    test('sdd', () => t(false));
  },
);

suite(
  'dag with some permanent hashes and some memory-only hashes on top w index',
  () => {
    const t = async (dd31: boolean) => {
      const clientID = 'client-id';
      const hashFunction = makeNewFakeHashFunction();
      const perdag = new dag.TestStore(undefined, hashFunction);
      const memdag = new dag.LazyStore(
        perdag,
        100 * 2 ** 20, // 100 MB,
        hashFunction,
        assertHash,
      );

      const mb = new ChainBuilder(memdag, undefined, dd31);
      const pb = new ChainBuilder(perdag, undefined, dd31);

      await pb.addGenesis(clientID, {
        testIndex: {prefix: '', jsonPointer: '/name', allowEmpty: true},
      });

      await pb.addSnapshot(
        Object.entries({
          a: 1,
          b: {name: 'b-name'},
        }),
        clientID,
        undefined,
        undefined,
      );
      await memdag.withWrite(async memdagWrite => {
        await memdagWrite.setHead(db.DEFAULT_HEAD_NAME, pb.headHash);
        await memdagWrite.commit();
      });

      mb.chain = pb.chain.slice();
      if (!dd31) {
        await mb.addIndexChange(clientID, 'testIndex', {
          prefix: '',
          jsonPointer: '/name',
          allowEmpty: true,
        });
      }
      await mb.addLocal(clientID, [['c', {name: 'c-name'}]]);

      await memdag.withRead(async dagRead => {
        const visitor = new GatherMemoryOnlyVisitor(dagRead);
        await visitor.visitCommit(mb.headHash);
        expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal(
          dd31
            ? {
                ['face0000000040008000000000000000' + '' + '000000000008']: {
                  hash:
                    'face0000000040008000000000000000' + '' + '000000000008',
                  data: {
                    meta: {
                      type: MetaType.LocalDD31,
                      basisHash:
                        'face0000000040008000000000000000' +
                        '' +
                        '000000000005',
                      baseSnapshotHash:
                        'face0000000040008000000000000000' +
                        '' +
                        '000000000005',
                      mutationID: 2,
                      mutatorName: 'mutator_name_2',
                      mutatorArgsJSON: [2],
                      originalHash: null,
                      timestamp: 42,
                      clientID: 'client-id',
                    },
                    valueHash:
                      'face0000000040008000000000000000' + '' + '000000000006',
                    indexes: [
                      {
                        definition: {
                          name: 'testIndex',
                          keyPrefix: '',
                          jsonPointer: '/name',
                          allowEmpty: true,
                        },
                        valueHash:
                          'face0000000040008000000000000000' +
                          '' +
                          '000000000007',
                      },
                    ],
                  },
                  meta: [
                    'face0000000040008000000000000000' + '' + '000000000006',
                    'face0000000040008000000000000000' + '' + '000000000005',
                    'face0000000040008000000000000000' + '' + '000000000007',
                  ],
                },
                ['face0000000040008000000000000000' + '' + '000000000006']: {
                  hash:
                    'face0000000040008000000000000000' + '' + '000000000006',
                  data: [
                    0,
                    [
                      ['a', 1],
                      [
                        'b',
                        {
                          name: 'b-name',
                        },
                      ],
                      [
                        'c',
                        {
                          name: 'c-name',
                        },
                      ],
                    ],
                  ],
                  meta: [],
                },
                ['face0000000040008000000000000000' + '' + '000000000007']: {
                  hash:
                    'face0000000040008000000000000000' + '' + '000000000007',
                  data: [
                    0,
                    [
                      [
                        '\u0000b-name\u0000b',
                        {
                          name: 'b-name',
                        },
                      ],
                      [
                        '\u0000c-name\u0000c',
                        {
                          name: 'c-name',
                        },
                      ],
                    ],
                  ],
                  meta: [],
                },
              }
            : {
                ['face0000000040008000000000000000' + '000000000006']: {
                  data: [
                    0,
                    [
                      [
                        '\u0000b-name\u0000b',
                        {
                          name: 'b-name',
                        },
                      ],
                    ],
                  ],
                  hash: 'face0000000040008000000000000000' + '000000000006',
                  meta: [],
                },
                ['face0000000040008000000000000000' + '000000000007']: {
                  data: {
                    indexes: [
                      {
                        definition: {
                          allowEmpty: true,
                          jsonPointer: '/name',
                          keyPrefix: '',
                          name: 'testIndex',
                        },
                        valueHash:
                          'face0000000040008000000000000000' + '000000000006',
                      },
                    ],
                    meta: {
                      basisHash:
                        'face0000000040008000000000000000' + '000000000005',
                      lastMutationID: 1,
                      type: 1,
                    },
                    valueHash:
                      'face0000000040008000000000000000' + '000000000003',
                  },
                  hash: 'face0000000040008000000000000000' + '000000000007',
                  meta: [
                    'face0000000040008000000000000000' + '000000000003',
                    'face0000000040008000000000000000' + '000000000005',
                    'face0000000040008000000000000000' + '000000000006',
                  ],
                },
                ['face0000000040008000000000000000' + '000000000008']: {
                  data: [
                    0,
                    [
                      ['a', 1],
                      [
                        'b',
                        {
                          name: 'b-name',
                        },
                      ],
                      [
                        'c',
                        {
                          name: 'c-name',
                        },
                      ],
                    ],
                  ],
                  hash: 'face0000000040008000000000000000' + '000000000008',
                  meta: [],
                },
                ['face0000000040008000000000000000' + '000000000009']: {
                  data: [
                    0,
                    [
                      [
                        '\u0000b-name\u0000b',
                        {
                          name: 'b-name',
                        },
                      ],
                      [
                        '\u0000c-name\u0000c',
                        {
                          name: 'c-name',
                        },
                      ],
                    ],
                  ],
                  hash: 'face0000000040008000000000000000' + '000000000009',
                  meta: [],
                },
                ['face0000000040008000000000000000' + '000000000010']: {
                  data: {
                    indexes: [
                      {
                        definition: {
                          allowEmpty: true,
                          jsonPointer: '/name',
                          keyPrefix: '',
                          name: 'testIndex',
                        },
                        valueHash:
                          'face0000000040008000000000000000' + '000000000009',
                      },
                    ],
                    meta: {
                      basisHash:
                        'face0000000040008000000000000000' + '000000000007',
                      mutationID: 2,
                      mutatorArgsJSON: [3],
                      mutatorName: 'mutator_name_3',
                      originalHash: null,
                      timestamp: 42,
                      type: 2,
                    },
                    valueHash:
                      'face0000000040008000000000000000' + '000000000008',
                  },
                  hash: 'face0000000040008000000000000000' + '000000000010',
                  meta: [
                    'face0000000040008000000000000000' + '000000000008',
                    'face0000000040008000000000000000' + '000000000007',
                    'face0000000040008000000000000000' + '000000000009',
                  ],
                },
              },
        );
      });
    };

    test('dd31', () => t(true));
    test('sdd', () => t(false));
  },
);
