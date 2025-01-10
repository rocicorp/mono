import {describe, expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.js';
import {LazyStore} from '../dag/lazy-store.js';
import {TestLazyStore} from '../dag/test-lazy-store.js';
import {TestStore} from '../dag/test-store.js';
import {DEFAULT_HEAD_NAME} from '../db/commit.js';
import * as MetaType from '../db/meta-type-enum.js';
import {ChainBuilder} from '../db/test-helpers.js';
import * as FormatVersion from '../format-version-enum.js';
import {assertHash, fakeHash, makeNewFakeHashFunction} from '../hash.js';
import {withRead, withWriteNoImplicitCommit} from '../with-transactions.js';
import {GatherMemoryOnlyVisitor} from './gather-mem-only-visitor.js';

describe('dag with no memory-only hashes gathers nothing', () => {
  const t = async (formatVersion: FormatVersion.Type) => {
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
    if (formatVersion >= FormatVersion.DD31) await pb.addLocal(clientID);

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
});

describe('dag with only memory-only hashes gathers everything', () => {
  const t = async (formatVersion: FormatVersion.Type) => {
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
    assert(formatVersion >= FormatVersion.DD31);

    await mb.addSnapshot(undefined, clientID);
    await testGatheredChunks();
  };

  test('dd31', () => t(FormatVersion.Latest));
});

describe('dag with some persisted hashes and some memory-only hashes on top', () => {
  const t = async (formatVersion: FormatVersion.Type) => {
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
        basisHash: fakeHash(3),
        mutationID: 2,
        mutatorArgsJSON: [2],
        mutatorName: 'mutator_name_2',
        originalHash: null,
        timestamp: 42,
      };
      assert(formatVersion >= FormatVersion.DD31);
      const meta = {
        type: MetaType.LocalDD31,
        ...metaBase,
        baseSnapshotHash: fakeHash(1),
        clientID,
      };
      expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal({
        [fakeHash(4)]: {
          data: [
            0,
            [
              formatVersion >= FormatVersion.V7
                ? ['local', '2', 27]
                : ['local', '2'],
            ],
          ],
          hash: fakeHash(4),
          meta: [],
        },
        [fakeHash(5)]: {
          data: {
            indexes: [],
            meta,
            valueHash: fakeHash(4),
          },
          hash: fakeHash(5),
          meta: [fakeHash(3), fakeHash(4)],
        },
      });
    });
  };
  test('dd31', () => t(FormatVersion.Latest));
});

describe('dag with some permanent hashes and some memory-only hashes on top w index', () => {
  const t = async (formatVersion: FormatVersion.Type) => {
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
    assert(formatVersion >= FormatVersion.DD31);
    await mb.addLocal(clientID, [['c', {name: 'c-name'}]]);

    await withRead(memdag, async dagRead => {
      const visitor = new GatherMemoryOnlyVisitor(dagRead);
      await visitor.visit(mb.headHash);
      expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal(
        formatVersion >= FormatVersion.DD31
          ? {
              [fakeHash(8)]: {
                hash: fakeHash(8),
                data: {
                  meta: {
                    type: MetaType.LocalDD31,
                    basisHash: fakeHash(5),
                    baseSnapshotHash: fakeHash(5),
                    mutationID: 2,
                    mutatorName: 'mutator_name_2',
                    mutatorArgsJSON: [2],
                    originalHash: null,
                    timestamp: 42,
                    clientID: 'client-id',
                  },
                  valueHash: fakeHash(6),
                  indexes: [
                    {
                      definition: {
                        name: 'testIndex',
                        keyPrefix: '',
                        jsonPointer: '/name',
                        allowEmpty: true,
                      },
                      valueHash: fakeHash(7),
                    },
                  ],
                },
                meta: [fakeHash(5), fakeHash(6), fakeHash(7)],
              },
              [fakeHash(6)]: {
                hash: fakeHash(6),
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
              [fakeHash(7)]: {
                hash: fakeHash(7),
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
              [fakeHash(6)]: {
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
                hash: fakeHash(6),
                meta: [],
              },
              [fakeHash(7)]: {
                data: {
                  indexes: [
                    {
                      definition: {
                        allowEmpty: true,
                        jsonPointer: '/name',
                        keyPrefix: '',
                        name: 'testIndex',
                      },
                      valueHash: fakeHash(6),
                    },
                  ],
                  meta: {
                    basisHash: fakeHash(5),
                    lastMutationID: 1,
                    type: 1,
                  },
                  valueHash: fakeHash(3),
                },
                hash: fakeHash(7),
                meta: [fakeHash(3), fakeHash(5), fakeHash(6)],
              },
              [fakeHash(8)]: {
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
                hash: fakeHash(8),
                meta: [],
              },
              [fakeHash(9)]: {
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
                hash: fakeHash(9),
                meta: [],
              },
              [fakeHash(10)]: {
                data: {
                  indexes: [
                    {
                      definition: {
                        allowEmpty: true,
                        jsonPointer: '/name',
                        keyPrefix: '',
                        name: 'testIndex',
                      },
                      valueHash: fakeHash(9),
                    },
                  ],
                  meta: {
                    basisHash: fakeHash(7),
                    mutationID: 2,
                    mutatorArgsJSON: [3],
                    mutatorName: 'mutator_name_3',
                    originalHash: null,
                    timestamp: 42,
                    type: 2,
                  },
                  valueHash: fakeHash(8),
                },
                hash: fakeHash(10),
                meta: [fakeHash(7), fakeHash(8), fakeHash(9)],
              },
            },
      );
    });
  };

  test('dd31', () => t(FormatVersion.Latest));
});
