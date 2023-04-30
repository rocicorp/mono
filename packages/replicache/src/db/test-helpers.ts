import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import {assert, assertNotUndefined} from 'shared/asserts.js';
import * as btree from '../btree/mod.js';
import {BTreeWrite} from '../btree/mod.js';
import type {Cookie} from '../cookies.js';
import type * as dag from '../dag/mod.js';
import {Visitor} from '../dag/visitor.js';
import {Hash, emptyHash} from '../hash.js';
import type {IndexDefinition, IndexDefinitions} from '../index-defs.js';
import {JSONValue, deepFreeze} from '../json.js';
import type {ClientID} from '../sync/ids.js';
import {addSyncSnapshot} from '../sync/test-helpers.js';
import {withRead, withWrite} from '../with-transactions.js';
import {
  ChunkIndexDefinition,
  Commit,
  DEFAULT_HEAD_NAME,
  IndexRecord,
  LocalMeta,
  Meta,
  MetaType,
  SnapshotMetaDD31,
  SnapshotMetaSDD,
  assertIndexChangeCommit,
  assertLocalCommitDD31,
  assertLocalCommitSDD,
  assertSnapshotCommitDD31,
  assertSnapshotCommitSDD,
  chunkIndexDefinitionEqualIgnoreName,
  fromHead,
  toChunkIndexDefinition,
} from './commit.js';
import {IndexWrite} from './index.js';
import {
  Whence,
  readCommit,
  readCommitForBTreeWrite,
  whenceHead,
} from './read.js';
import {
  Write,
  createIndexBTree,
  newWriteLocal,
  newWriteSnapshotDD31,
  newWriteSnapshotSDD,
  readIndexesForWrite,
} from './write.js';

export type Chain = Commit<Meta>[];

async function addGenesis(
  chain: Chain,
  store: dag.Store,
  clientID: ClientID,
  headName = DEFAULT_HEAD_NAME,
  indexDefinitions: IndexDefinitions,
  dd31: boolean,
): Promise<Chain> {
  expect(chain).to.have.length(0);
  const commit = await createGenesis(
    store,
    clientID,
    headName,
    indexDefinitions,
    dd31,
  );
  chain.push(commit);
  return chain;
}

async function createGenesis(
  store: dag.Store,
  clientID: ClientID,
  headName: string,
  indexDefinitions: IndexDefinitions,
  dd31: boolean,
): Promise<Commit<Meta>> {
  await withWrite(store, async w => {
    await initDB(w, headName, clientID, indexDefinitions, dd31);
  });
  return withRead(store, async read => {
    const [, commit] = await readCommit(whenceHead(headName), read);
    return commit;
  });
}

// Local commit has mutator name and args according to its index in the
// chain.
async function addLocal(
  chain: Chain,
  store: dag.Store,
  clientID: ClientID,
  entries: [string, JSONValue][] | undefined,
  headName: string,
  dd31: boolean,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const i = chain.length;
  const commit = await createLocal(
    entries ?? [[`local`, `${i}`]],
    store,
    i,
    clientID,
    headName,
    dd31,
  );

  chain.push(commit);
  return chain;
}

async function createLocal(
  entries: [string, JSONValue][],
  store: dag.Store,
  i: number,
  clientID: ClientID,
  headName: string,
  dd31: boolean,
): Promise<Commit<Meta>> {
  const lc = new LogContext();
  await withWrite(store, async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(headName),
      createMutatorName(i),
      deepFreeze([i]),
      null,
      dagWrite,
      42,
      clientID,
      dd31,
    );
    for (const [key, val] of entries) {
      await w.put(lc, key, deepFreeze(val));
    }
    await w.commit(headName);
  });
  return withRead(store, dagRead => fromHead(headName, dagRead));
}

export function createMutatorName(chainIndex: number): string {
  return `mutator_name_${chainIndex}`;
}

async function addIndexChange(
  chain: Chain,
  store: dag.Store,
  clientID: ClientID,
  indexName: string | undefined,
  indexDefinition: IndexDefinition | undefined,
  headName: string,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const i = chain.length;
  const name = indexName ?? `${i}`;
  const {
    prefix = 'local',
    jsonPointer = '',
    allowEmpty = false,
  } = indexDefinition ?? {};

  const commit = await createIndex(
    store,
    clientID,
    name,
    prefix,
    jsonPointer,
    allowEmpty,
    headName,
  );
  chain.push(commit);
  return chain;
}

async function createIndex(
  store: dag.Store,
  clientID: ClientID,
  name: string,
  prefix: string,
  jsonPointer: string,
  allowEmpty: boolean,
  headName: string,
): Promise<Commit<Meta>> {
  const lc = new LogContext();
  await withWrite(store, async dagWrite => {
    const w = await newWriteIndexChange(
      whenceHead(headName),
      dagWrite,
      clientID,
    );
    await createIndexForTesting(
      lc,
      name,
      prefix,
      jsonPointer,
      allowEmpty,
      w.indexes,
      dagWrite,
      w.map,
    );
    await w.commit(headName);
  });
  return withRead(store, async dagRead => {
    const [, commit] = await readCommit(whenceHead(headName), dagRead);
    return commit;
  });
}

// See also sync.test_helpers for addSyncSnapshot, which can't go here because
// it depends on details of sync and sync depends on db.

// The optional map for the commit is treated as key, value pairs.
async function addSnapshot(
  chain: Chain,
  store: dag.Store,
  map: [string, JSONValue][] | undefined,
  clientID: ClientID,
  cookie: Cookie = `cookie_${chain.length}`,
  lastMutationIDs: Record<ClientID, number> | undefined,
  headName: string,
  dd31: boolean,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const lc = new LogContext();
  await withWrite(store, async dagWrite => {
    let w;
    if (dd31) {
      w = await newWriteSnapshotDD31(
        whenceHead(headName),
        lastMutationIDs ?? {
          [clientID]: await chain[chain.length - 1].getNextMutationID(
            clientID,
            dagWrite,
          ),
        },
        deepFreeze(cookie),
        dagWrite,
        clientID,
      );
    } else {
      w = await newWriteSnapshotSDD(
        whenceHead(DEFAULT_HEAD_NAME),
        await chain[chain.length - 1].getNextMutationID(clientID, dagWrite),
        deepFreeze(cookie),
        dagWrite,
        readIndexesForWrite(chain[chain.length - 1], dagWrite),
        clientID,
      );
    }

    if (map) {
      for (const [k, v] of map) {
        await w.put(lc, k, deepFreeze(v));
      }
    }
    await w.commit(headName);
  });
  return withRead(store, async dagRead => {
    const [, commit] = await readCommit(whenceHead(headName), dagRead);
    chain.push(commit);
    return chain;
  });
}

export class ChainBuilder {
  readonly store: dag.Store;
  readonly headName: string;
  chain: Chain;
  readonly dd31: boolean;

  constructor(store: dag.Store, headName = DEFAULT_HEAD_NAME, dd31 = true) {
    this.store = store;
    this.headName = headName;
    this.chain = [];
    this.dd31 = dd31;
  }

  async addGenesis(
    clientID: ClientID,
    indexDefinitions: IndexDefinitions = {},
  ): Promise<Commit<SnapshotMetaSDD | SnapshotMetaDD31>> {
    await addGenesis(
      this.chain,
      this.store,
      clientID,
      this.headName,
      indexDefinitions,
      this.dd31,
    );
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    if (this.dd31) {
      assertSnapshotCommitDD31(commit);
    } else {
      assertSnapshotCommitSDD(commit);
    }
    return commit;
  }

  async addLocal(
    clientID: ClientID,
    entries?: [string, JSONValue][],
  ): Promise<Commit<LocalMeta>> {
    await addLocal(
      this.chain,
      this.store,
      clientID,
      entries,
      this.headName,
      this.dd31,
    );
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    if (this.dd31) {
      assertLocalCommitDD31(commit);
    } else {
      assertLocalCommitSDD(commit);
    }
    return commit;
  }

  async addSnapshot(
    map: [string, JSONValue][] | undefined,
    clientID: ClientID,
    cookie: Cookie = `cookie_${this.chain.length}`,
    lastMutationIDs?: Record<ClientID, number>,
  ): Promise<Commit<SnapshotMetaSDD | SnapshotMetaDD31>> {
    await addSnapshot(
      this.chain,
      this.store,
      map,
      clientID,
      cookie,
      lastMutationIDs,
      this.headName,
      this.dd31,
    );
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    if (this.dd31) {
      assertSnapshotCommitDD31(commit);
    } else {
      assertSnapshotCommitSDD(commit);
    }
    return commit;
  }

  async addIndexChange(
    clientID: ClientID,
    indexName?: string,
    indexDefinition?: IndexDefinition,
  ): Promise<Commit<Meta>> {
    assert(!this.dd31);
    await addIndexChange(
      this.chain,
      this.store,
      clientID,
      indexName,
      indexDefinition,
      this.headName,
    );
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    assertIndexChangeCommit(commit);
    return commit;
  }

  addSyncSnapshot(takeIndexesFrom: number, clientID: ClientID) {
    return addSyncSnapshot(
      this.chain,
      this.store,
      takeIndexesFrom,
      clientID,
      this.dd31,
    );
  }

  async removeHead(): Promise<void> {
    await withWrite(this.store, async write => {
      await write.removeHead(this.headName);
      await write.commit();
    });
  }

  get headHash(): Hash {
    const lastCommit = this.chain.at(-1);
    assert(lastCommit);
    return lastCommit.chunk.hash;
  }
}

export async function initDB(
  dagWrite: dag.Write,
  headName: string,
  clientID: ClientID,
  indexDefinitions: IndexDefinitions,
  dd31: boolean,
): Promise<Hash> {
  const basisHash = emptyHash;
  const indexes = await createEmptyIndexMaps(indexDefinitions, dagWrite);
  const meta = dd31
    ? ({
        basisHash,
        type: MetaType.SnapshotDD31,
        lastMutationIDs: {},
        cookieJSON: null,
      } as const)
    : ({
        basisHash,
        type: MetaType.SnapshotSDD,
        lastMutationID: 0,
        cookieJSON: null,
      } as const);

  const w = new Write(
    dagWrite,
    new BTreeWrite(dagWrite),
    undefined,
    meta,
    indexes,
    clientID,
    dd31,
  );
  return w.commit(headName);
}

async function createEmptyIndexMaps(
  indexDefinitions: IndexDefinitions,
  dagWrite: dag.Write,
): Promise<Map<string, IndexWrite>> {
  const indexes = new Map();

  let emptyTreeHash: Hash | undefined;
  for (const [name, indexDefinition] of Object.entries(indexDefinitions)) {
    if (!emptyTreeHash) {
      const emptyBTreeChunk = dagWrite.createChunk(btree.emptyDataNode, []);
      await dagWrite.putChunk(emptyBTreeChunk);
      emptyTreeHash = emptyBTreeChunk.hash;
    }
    const indexRecord: IndexRecord = {
      definition: toChunkIndexDefinition(name, indexDefinition),
      valueHash: emptyTreeHash,
    };
    indexes.set(
      name,
      new IndexWrite(indexRecord, new BTreeWrite(dagWrite, emptyTreeHash)),
    );
  }
  return indexes;
}

class ChunkSnapshotVisitor extends Visitor {
  snapshot: Record<string, unknown> = {};

  override visitChunk(chunk: dag.Chunk): Promise<void> {
    this.snapshot[chunk.hash.toString()] = chunk.data;
    return super.visitChunk(chunk);
  }
}

export function getChunkSnapshot(
  dagStore: dag.Store,
  hash: Hash,
): Promise<Record<string, unknown>> {
  return withRead(dagStore, async dagRead => {
    const v = new ChunkSnapshotVisitor(dagRead);
    await v.visit(hash);
    return v.snapshot;
  });
}

async function newWriteIndexChange(
  whence: Whence,
  dagWrite: dag.Write,
  clientID: ClientID,
): Promise<Write> {
  const [basisHash, basis, bTreeWrite] = await readCommitForBTreeWrite(
    whence,
    dagWrite,
  );
  const lastMutationID = await basis.getMutationID(clientID, dagWrite);
  const indexes = readIndexesForWrite(basis, dagWrite);
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.IndexChangeSDD, lastMutationID},
    indexes,
    clientID,
    false,
  );
}

async function createIndexForTesting(
  lc: LogContext,
  name: string,
  prefix: string,
  jsonPointer: string,
  allowEmpty: boolean,
  indexes: Map<string, IndexWrite>,
  dagWrite: dag.Write,
  map: btree.BTreeRead,
): Promise<void> {
  const chunkIndexDefinition: ChunkIndexDefinition = {
    name,
    keyPrefix: prefix,
    jsonPointer,
    allowEmpty,
  };

  // Check to see if the index already exists.
  const index = indexes.get(name);
  if (index) {
    if (
      // Name already checked
      !chunkIndexDefinitionEqualIgnoreName(
        chunkIndexDefinition,
        index.meta.definition,
      )
    ) {
      throw new Error('Index exists with different definition');
    }
  }

  const indexMap = await createIndexBTree(
    lc,
    dagWrite,
    map,
    prefix,
    jsonPointer,
    allowEmpty,
  );

  indexes.set(
    name,
    new IndexWrite(
      {
        definition: chunkIndexDefinition,
        valueHash: emptyHash,
      },
      indexMap,
    ),
  );
}
