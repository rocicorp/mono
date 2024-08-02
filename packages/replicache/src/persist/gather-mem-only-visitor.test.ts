import {expect} from 'chai';
import {LazyStore} from '../dag/lazy-store.js';
import {TestLazyStore} from '../dag/test-lazy-store.js';
import {TestStore} from '../dag/test-store.js';
import {DEFAULT_HEAD_NAME, MetaType} from '../db/commit.js';
import {ChainBuilder} from '../db/test-helpers.js';
import {FormatVersion} from '../format-version.js';
import {assertHash, makeNewFakeHashFunction} from '../hash.js';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.js';
import {GatherMemoryOnlyVisitor} from './gather-mem-only-visitor.js';

suite('dag with no memory-only hashes gathers nothing', () => {
  const t = async (formatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const hashFunction = makeNewFakeHashFunction();
    const perdag = new TestStore(undefined, hashFunction);
    const memdag = new TestLazyStore(
      perdag,
      100 * 2 ** 20, // 100 MB,
      hashFunction,
      assertHash,
    );

    const pb = new ChainBuilder(perdag, undefined, formatVersion);
    await pb.addGenesis(clientID);
    await pb.addLocal(clientID);
    if (formatVersion <= FormatVersion.SDD) {
      await pb.addIndexChange(clientID);
    }
    await pb.addLocal(clientID);

    await withRead(memdag, async dagRead => {
      for (const commit of pb.chain) {
        const visitor = new GatherMemoryOnlyVisitor(dagRead);
        await visitor.visit(commit.chunk.hash);
        expect(visitor.gatheredChunks).to.be.empty;
      }
    });

    await pb.addSnapshot(undefined, clientID);

    await withRead(memdag, async dagRead => {
      const visitor = new GatherMemoryOnlyVisitor(dagRead);
      await visitor.visit(pb.headHash);
      expect(visitor.gatheredChunks).to.be.empty;
    });
  };

  test('dd31', () => t(FormatVersion.Latest));
  test('sdd', () => t(FormatVersion.SDD));
});

suite('dag with only memory-only hashes gathers everything', () => {
  const t = async (formatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const hashFunction = makeNewFakeHashFunction();
    const perdag = new TestStore(undefined, hashFunction);
    const memdag = new TestLazyStore(
      perdag,
      100 * 2 ** 20, // 100 MB,
      hashFunction,
      assertHash,
    );

    const mb = new ChainBuilder(memdag, undefined, formatVersion);

    const testGatheredChunks = async () => {
      await withRead(memdag, async dagRead => {
        const visitor = new GatherMemoryOnlyVisitor(dagRead);
        await visitor.visit(mb.headHash);
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
    if (formatVersion <= FormatVersion.SDD) {
      await mb.addIndexChange(clientID);
    }

    await mb.addSnapshot(undefined, clientID);
    await testGatheredChunks();
  };

  test('dd31', () => t(FormatVersion.Latest));
  test('sdd', () => t(FormatVersion.SDD));
});

suite(
  'dag with some persisted hashes and some memory-only hashes on top',
  () => {
    const t = async (formatVersion: FormatVersion) => {
      const clientID = 'client-id';
      const hashFunction = makeNewFakeHashFunction();
      const perdag = new TestStore(undefined, hashFunction);
      const memdag = new LazyStore(
        perdag,
        100 * 2 ** 20, // 100 MB,
        hashFunction,
        assertHash,
      );

      const pb = new ChainBuilder(perdag, undefined, formatVersion);
      const mb = new ChainBuilder(memdag, undefined, formatVersion);

      await pb.addGenesis(clientID);
      await pb.addLocal(clientID);

      await withWriteNoImplicitCommit(memdag, async memdagWrite => {
        await memdagWrite.setHead(DEFAULT_HEAD_NAME, pb.headHash);
        await memdagWrite.commit();
      });
      mb.chain = pb.chain.slice();
      await mb.addLocal(clientID);

      await withRead(memdag, async dagRead => {
        const visitor = new GatherMemoryOnlyVisitor(dagRead);
        await visitor.visit(mb.headHash);
        const metaBase = {
          basisHash: 'face0000000040008000000000000000000000000003',
          mutationID: 2,
          mutatorArgsJSON: [2],
          mutatorName: 'mutator_name_2',
          originalHash: null,
          timestamp: 42,
        };
        const meta =
          formatVersion >= FormatVersion.DD31
            ? {
                type: MetaType.LocalDD31,
                ...metaBase,
                baseSnapshotHash:
                  'face0000000040008000000000000000000000000001',
                clientID,
              }
            : {type: MetaType.LocalSDD, ...metaBase};
        expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal({
          ['face0000000040008000000000000000000000000004']: {
            data: [
              0,
              [
                formatVersion >= FormatVersion.V7
                  ? ['local', '2', 27]
                  : ['local', '2'],
              ],
            ],
            hash: 'face0000000040008000000000000000000000000004',
            meta: [],
          },
          ['face0000000040008000000000000000000000000005']: {
            data: {
              indexes: [],
              meta,
              valueHash: 'face0000000040008000000000000000000000000004',
            },
            hash: 'face0000000040008000000000000000000000000005',
            meta: [
              'face0000000040008000000000000000000000000003',
              'face0000000040008000000000000000000000000004',
            ],
          },
        });
      });
    };
    test('dd31', () => t(FormatVersion.Latest));
    test('sdd', () => t(FormatVersion.SDD));
  },
);

suite(
  'dag with some permanent hashes and some memory-only hashes on top w index',
  () => {
    const t = async (formatVersion: FormatVersion) => {
      const clientID = 'client-id';
      const hashFunction = makeNewFakeHashFunction();
      const perdag = new TestStore(undefined, hashFunction);
      const memdag = new LazyStore(
        perdag,
        100 * 2 ** 20, // 100 MB,
        hashFunction,
        assertHash,
      );

      const mb = new ChainBuilder(memdag, undefined, formatVersion);
      const pb = new ChainBuilder(perdag, undefined, formatVersion);

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
      await withWriteNoImplicitCommit(memdag, async memdagWrite => {
        await memdagWrite.setHead(DEFAULT_HEAD_NAME, pb.headHash);
        await memdagWrite.commit();
      });

      mb.chain = pb.chain.slice();
      if (formatVersion <= FormatVersion.SDD) {
        await mb.addIndexChange(clientID, 'testIndex', {
          prefix: '',
          jsonPointer: '/name',
          allowEmpty: true,
        });
      }
      await mb.addLocal(clientID, [['c', {name: 'c-name'}]]);

      await withRead(memdag, async dagRead => {
        const visitor = new GatherMemoryOnlyVisitor(dagRead);
        await visitor.visit(mb.headHash);
        expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal(
          formatVersion >= FormatVersion.DD31
            ? {
                ['face0000000040008000000000000000000000000008']: {
                  hash: 'face0000000040008000000000000000000000000008',
                  data: {
                    meta: {
                      type: MetaType.LocalDD31,
                      basisHash: 'face0000000040008000000000000000000000000005',
                      baseSnapshotHash:
                        'face0000000040008000000000000000000000000005',
                      mutationID: 2,
                      mutatorName: 'mutator_name_2',
                      mutatorArgsJSON: [2],
                      originalHash: null,
                      timestamp: 42,
                      clientID: 'client-id',
                    },
                    valueHash: 'face0000000040008000000000000000000000000006',
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
                    'face0000000040008000000000000000000000000005',
                    'face0000000040008000000000000000000000000006',
                    'face0000000040008000000000000000000000000007',
                  ],
                },
                ['face0000000040008000000000000000000000000006']: {
                  hash: 'face0000000040008000000000000000000000000006',
                  data: [
                    0,
                    [
                      ['a', 1, 22],
                      [
                        'b',
                        {
                          name: 'b-name',
                        },
                        43,
                      ],
                      [
                        'c',
                        {
                          name: 'c-name',
                        },
                        43,
                      ],
                    ],
                  ],
                  meta: [],
                },
                ['face0000000040008000000000000000000000000007']: {
                  hash: 'face0000000040008000000000000000000000000007',
                  data: [
                    0,
                    [
                      [
                        '\u0000b-name\u0000b',
                        {
                          name: 'b-name',
                        },
                        51,
                      ],
                      [
                        '\u0000c-name\u0000c',
                        {
                          name: 'c-name',
                        },
                        51,
                      ],
                    ],
                  ],
                  meta: [],
                },
              }
            : {
                ['face0000000040008000000000000000000000000006']: {
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
                  hash: 'face0000000040008000000000000000000000000006',
                  meta: [],
                },
                ['face0000000040008000000000000000000000000007']: {
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
                          'face0000000040008000000000000000000000000006',
                      },
                    ],
                    meta: {
                      basisHash: 'face0000000040008000000000000000000000000005',
                      lastMutationID: 1,
                      type: 1,
                    },
                    valueHash: 'face0000000040008000000000000000000000000003',
                  },
                  hash: 'face0000000040008000000000000000000000000007',
                  meta: [
                    'face0000000040008000000000000000000000000003',
                    'face0000000040008000000000000000000000000005',
                    'face0000000040008000000000000000000000000006',
                  ],
                },
                ['face0000000040008000000000000000000000000008']: {
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
                  hash: 'face0000000040008000000000000000000000000008',
                  meta: [],
                },
                ['face0000000040008000000000000000000000000009']: {
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
                  hash: 'face0000000040008000000000000000000000000009',
                  meta: [],
                },
                ['face0000000040008000000000000000000000000010']: {
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
                          'face0000000040008000000000000000000000000009',
                      },
                    ],
                    meta: {
                      basisHash: 'face0000000040008000000000000000000000000007',
                      mutationID: 2,
                      mutatorArgsJSON: [3],
                      mutatorName: 'mutator_name_3',
                      originalHash: null,
                      timestamp: 42,
                      type: 2,
                    },
                    valueHash: 'face0000000040008000000000000000000000000008',
                  },
                  hash: 'face0000000040008000000000000000000000000010',
                  meta: [
                    'face0000000040008000000000000000000000000007',
                    'face0000000040008000000000000000000000000008',
                    'face0000000040008000000000000000000000000009',
                  ],
                },
              },
        );
      });
    };

    test('dd31', () => t(FormatVersion.Latest));
    test('sdd', () => t(FormatVersion.SDD));
  },
);
