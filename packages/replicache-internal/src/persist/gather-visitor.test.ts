import {expect} from '@esm-bundle/chai';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import {assertHash, makeNewFakeHashFunction} from '../hash';
import {
  addGenesis,
  addIndexChange,
  addLocal,
  addSnapshot,
  Chain,
} from '../db/test-helpers';
import {GatherVisitor} from './gather-visitor';
import type {JSONObject} from '../json.js';

test('dag with no memory-only hashes gathers nothing', async () => {
  const clientID = 'client-id';
  const hashFunction = makeNewFakeHashFunction();
  const perdag = new dag.TestStore(undefined, hashFunction);
  const memdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    hashFunction,
    assertHash,
  );

  const chain: Chain = [];
  await addGenesis(chain, perdag, clientID);
  await addLocal(chain, perdag, clientID);
  if (!DD31) {
    await addIndexChange(chain, perdag, clientID);
  }
  await addLocal(chain, perdag, clientID);

  await memdag.withRead(async dagRead => {
    for (const commit of chain) {
      const visitor = new GatherVisitor(dagRead);
      await visitor.visitCommit(commit.chunk.hash);
      expect(visitor.gatheredChunks).to.be.empty;
    }
  });

  await addSnapshot(chain, perdag, undefined, clientID);

  await memdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(visitor.gatheredChunks).to.be.empty;
  });
});

test('dag with only temp hashes gathers everything', async () => {
  const clientID = 'client-id';
  const hashFunction = makeNewFakeHashFunction();
  const perdag = new dag.TestStore(undefined, hashFunction);
  const memdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    hashFunction,
    assertHash,
  );
  const chain: Chain = [];

  const testGatheredChunks = async () => {
    await memdag.withRead(async dagRead => {
      const visitor = new GatherVisitor(dagRead);
      await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
      expect(memdag.getMemOnlyChunksSnapshot()).to.deep.equal(
        visitor.gatheredChunks,
      );
    });
  };

  await addGenesis(chain, memdag, clientID);
  await addLocal(chain, memdag, clientID);
  await testGatheredChunks();

  if (!DD31) {
    await addIndexChange(chain, memdag, clientID);
  }
  await addLocal(chain, memdag, clientID);
  await testGatheredChunks();

  await addSnapshot(chain, memdag, undefined, clientID);
  await testGatheredChunks();
});

test('dag with some permanent hashes and some memory-only hashes on top', async () => {
  const clientID = 'client-id';
  const hashFunction = makeNewFakeHashFunction();
  const perdag = new dag.TestStore(undefined, hashFunction);
  const memdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    hashFunction,
    assertHash,
  );
  const chain: Chain = [];

  await addGenesis(chain, perdag, clientID);
  await addLocal(chain, perdag, clientID);

  await memdag.withWrite(async memdagWrite => {
    await memdagWrite.setHead(
      db.DEFAULT_HEAD_NAME,
      chain[chain.length - 1].chunk.hash,
    );
    await memdagWrite.commit();
  });
  await addLocal(chain, memdag, clientID);

  await memdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    const meta: JSONObject = {
      basisHash: 'face0000-0000-4000-8000-000000000003',
      mutationID: 2,
      mutatorArgsJSON: [2],
      mutatorName: 'mutator_name_2',
      originalHash: null,
      timestamp: 42,
      type: 2,
    };
    if (DD31) {
      meta.clientID = clientID;
    }
    expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal({
      'face0000-0000-4000-8000-000000000004': {
        data: [0, [['local', '2']]],
        hash: 'face0000-0000-4000-8000-000000000004',
        meta: [],
      },
      'face0000-0000-4000-8000-000000000005': {
        data: {
          indexes: [],
          meta,
          valueHash: 'face0000-0000-4000-8000-000000000004',
        },
        hash: 'face0000-0000-4000-8000-000000000005',
        meta: [
          'face0000-0000-4000-8000-000000000004',
          'face0000-0000-4000-8000-000000000003',
        ],
      },
    });
  });
});

test('dag with some permanent hashes and some memory-only hashes on top w index', async () => {
  const clientID = 'client-id';
  const hashFunction = makeNewFakeHashFunction();
  const perdag = new dag.TestStore(undefined, hashFunction);
  const memdag = new dag.LazyStore(
    perdag,
    100 * 2 ** 20, // 100 MB,
    hashFunction,
    assertHash,
  );
  const chain: Chain = [];

  await addGenesis(chain, perdag, clientID);
  if (DD31) {
    await addSnapshot(
      chain,
      perdag,
      Object.entries({
        a: 1,
        b: {name: 'b-name'},
      }),
      clientID,
      undefined,
      undefined,
      {testIndex: {prefix: '', jsonPointer: '/name', allowEmpty: true}},
    );
    await memdag.withWrite(async memdagWrite => {
      await memdagWrite.setHead(
        db.DEFAULT_HEAD_NAME,
        chain[chain.length - 1].chunk.hash,
      );
      await memdagWrite.commit();
    });
    await addLocal(chain, memdag, clientID, [['c', {name: 'c-name'}]]);
  } else {
    await addSnapshot(
      chain,
      perdag,
      Object.entries({
        a: 1,
        b: {name: 'b-name'},
      }),
      clientID,
    );
    await memdag.withWrite(async memdagWrite => {
      await memdagWrite.setHead(
        db.DEFAULT_HEAD_NAME,
        chain[chain.length - 1].chunk.hash,
      );
      await memdagWrite.commit();
    });
    await addIndexChange(chain, memdag, clientID, 'testIndex', {
      prefix: '',
      jsonPointer: '/name',
      allowEmpty: true,
    });
    await addLocal(chain, memdag, clientID, [['c', {name: 'c-name'}]]);
  }

  await memdag.withRead(async dagRead => {
    const visitor = new GatherVisitor(dagRead);
    await visitor.visitCommit(chain[chain.length - 1].chunk.hash);
    expect(Object.fromEntries(visitor.gatheredChunks)).to.deep.equal(
      DD31
        ? {
            'face0000-0000-4000-8000-000000000008': {
              hash: 'face0000-0000-4000-8000-000000000008',
              data: {
                meta: {
                  type: 2,
                  basisHash: 'face0000-0000-4000-8000-000000000005',
                  mutationID: 2,
                  mutatorName: 'mutator_name_2',
                  mutatorArgsJSON: [2],
                  originalHash: null,
                  timestamp: 42,
                  clientID: 'client-id',
                },
                valueHash: 'face0000-0000-4000-8000-000000000006',
                indexes: [
                  {
                    definition: {
                      name: 'testIndex',
                      keyPrefix: '',
                      jsonPointer: '/name',
                      allowEmpty: true,
                    },
                    valueHash: 'face0000-0000-4000-8000-000000000007',
                  },
                ],
              },
              meta: [
                'face0000-0000-4000-8000-000000000006',
                'face0000-0000-4000-8000-000000000005',
                'face0000-0000-4000-8000-000000000007',
              ],
            },
            'face0000-0000-4000-8000-000000000006': {
              hash: 'face0000-0000-4000-8000-000000000006',
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
            'face0000-0000-4000-8000-000000000007': {
              hash: 'face0000-0000-4000-8000-000000000007',
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
            'face0000-0000-4000-8000-000000000008': {
              hash: 'face0000-0000-4000-8000-000000000008',
              data: {
                meta: {
                  type: 2,
                  basisHash: 'face0000-0000-4000-8000-000000000005',
                  mutationID: 2,
                  mutatorName: 'mutator_name_3',
                  mutatorArgsJSON: [3],
                  originalHash: null,
                  timestamp: 42,
                },
                valueHash: 'face0000-0000-4000-8000-000000000006',
                indexes: [
                  {
                    definition: {
                      name: 'testIndex',
                      keyPrefix: '',
                      jsonPointer: '/name',
                      allowEmpty: true,
                    },
                    valueHash: 'face0000-0000-4000-8000-000000000007',
                  },
                ],
              },
              meta: [
                'face0000-0000-4000-8000-000000000006',
                'face0000-0000-4000-8000-000000000005',
                'face0000-0000-4000-8000-000000000007',
              ],
            },
            'face0000-0000-4000-8000-000000000005': {
              hash: 'face0000-0000-4000-8000-000000000005',
              data: {
                meta: {
                  type: 1,
                  basisHash: 'face0000-0000-4000-8000-000000000003',
                  lastMutationID: 1,
                },
                valueHash: 'face0000-0000-4000-8000-000000000002',
                indexes: [
                  {
                    definition: {
                      name: 'testIndex',
                      keyPrefix: '',
                      jsonPointer: '/name',
                      allowEmpty: true,
                    },
                    valueHash: 'face0000-0000-4000-8000-000000000004',
                  },
                ],
              },
              meta: [
                'face0000-0000-4000-8000-000000000002',
                'face0000-0000-4000-8000-000000000003',
                'face0000-0000-4000-8000-000000000004',
              ],
            },
            'face0000-0000-4000-8000-000000000006': {
              hash: 'face0000-0000-4000-8000-000000000006',
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
            'face0000-0000-4000-8000-000000000007': {
              hash: 'face0000-0000-4000-8000-000000000007',
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
            'face0000-0000-4000-8000-000000000004': {
              hash: 'face0000-0000-4000-8000-000000000004',
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
              meta: [],
            },
          },
    );
  });
});
