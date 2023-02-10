import {LogContext} from '@rocicorp/logger';
import {expect} from '@esm-bundle/chai';
import type * as dag from '../dag/mod';
import {
  assertLocalCommitDD31,
  assertSnapshotCommitDD31,
  Commit,
  CommitData,
  DEFAULT_HEAD_NAME,
  fromHead,
  IndexRecord,
  LocalMetaDD31,
  Meta,
  MetaType,
  nameIndexDefinition,
  SnapshotMetaDD31,
} from './commit';
import {readCommit, whenceHead} from './read';
import {
  Write,
  readIndexesForWrite,
  newWriteSnapshot,
  newWriteSnapshotDD31,
  newWriteLocal,
  newWriteIndexChange,
  createIndexBTree,
} from './write';
import type {JSONValue} from '../json';
import {toInternalValue, ToInternalValueReason} from '../internal-value.js';
import type {ClientID} from '../sync/client-id.js';
import {emptyHash, Hash} from '../hash.js';
import {BTreeRead, BTreeWrite, Node} from '../btree/mod.js';
import * as btree from '../btree/mod.js';
import type {IndexDefinition, IndexDefinitions} from '../index-defs.js';
import {IndexWrite} from './index.js';
import {Visitor} from './visitor';
import {assert, assertNotUndefined} from '../asserts';

export type Chain = Commit<Meta>[];

export async function addGenesis(
  chain: Chain,
  store: dag.Store,
  clientID: ClientID,
  headName = DEFAULT_HEAD_NAME,
): Promise<Chain> {
  expect(chain).to.have.length(0);
  const commit = await createGenesis(store, clientID, headName);
  chain.push(commit);
  return chain;
}

export async function createGenesis(
  store: dag.Store,
  clientID: ClientID,
  headName = DEFAULT_HEAD_NAME,
): Promise<Commit<Meta>> {
  await store.withWrite(async w => {
    await initDB(w, headName, clientID);
  });
  return await store.withRead(async read => {
    const [, commit] = await readCommit(whenceHead(headName), read);
    return commit;
  });
}

// Local commit has mutator name and args according to its index in the
// chain.
export async function addLocal(
  chain: Chain,
  store: dag.Store,
  clientID: ClientID,
  entries?: [string, JSONValue][],
  headName = DEFAULT_HEAD_NAME,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const i = chain.length;
  const commit = await createLocal(
    entries ?? [[`local`, `${i}`]],
    store,
    i,
    clientID,
    headName,
  );

  chain.push(commit);
  return chain;
}

export async function createLocal(
  entries: [string, JSONValue][],
  store: dag.Store,
  i: number,
  clientID: ClientID,
  headName = DEFAULT_HEAD_NAME,
): Promise<Commit<Meta>> {
  const lc = new LogContext();
  await store.withWrite(async dagWrite => {
    const w = await newWriteLocal(
      whenceHead(headName),
      createMutatorName(i),
      toInternalValue([i], ToInternalValueReason.Test),
      null,
      dagWrite,
      42,
      clientID,
    );
    for (const [key, val] of entries) {
      await w.put(lc, key, toInternalValue(val, ToInternalValueReason.Test));
    }
    await w.commit(headName);
  });
  return store.withRead(dagRead => fromHead(headName, dagRead));
}

export function createMutatorName(chainIndex: number): string {
  return `mutator_name_${chainIndex}`;
}

export async function addIndexChange(
  chain: Chain,
  store: dag.Store,
  clientID: ClientID,
  indexName?: string,
  indexDefinition?: IndexDefinition,
  headName = DEFAULT_HEAD_NAME,
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
    name,
    prefix,
    jsonPointer,
    store,
    allowEmpty,
    clientID,
    headName,
  );
  chain.push(commit);
  return chain;
}

export async function createIndex(
  name: string,
  prefix: string,
  jsonPointer: string,
  store: dag.Store,
  allowEmpty: boolean,
  clientID: ClientID,
  headName = DEFAULT_HEAD_NAME,
): Promise<Commit<Meta>> {
  assert(!DD31);
  const lc = new LogContext();
  await store.withWrite(async dagWrite => {
    const w = await newWriteIndexChange(
      whenceHead(headName),
      dagWrite,
      clientID,
    );
    await w.createIndex(lc, name, prefix, jsonPointer, allowEmpty);
    await w.commit(headName);
  });
  return store.withRead(async dagRead => {
    const [, commit] = await readCommit(whenceHead(headName), dagRead);
    return commit;
  });
}

// See also sync.test_helpers for addSyncSnapshot, which can't go here because
// it depends on details of sync and sync depends on db.

// The optional map for the commit is treated as key, value pairs.
export async function addSnapshot(
  chain: Chain,
  store: dag.Store,
  map: [string, JSONValue][] | undefined,
  clientID: ClientID,
  cookie: JSONValue = `cookie_${chain.length}`,
  lastMutationIDs?: Record<ClientID, number>,
  indexDefinitions?: IndexDefinitions,
  headName = DEFAULT_HEAD_NAME,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const lc = new LogContext();
  await store.withWrite(async dagWrite => {
    let w;
    let indexes: Map<string, IndexWrite>;
    if (DD31) {
      if (indexDefinitions) {
        indexes = new Map();
        for (const [name, indexDefinition] of Object.entries(
          indexDefinitions,
        )) {
          const valueMap = new BTreeRead(
            dagWrite,
            chain[chain.length - 1].valueHash,
          );
          const indexMap = await createIndexBTree(
            new LogContext(),
            dagWrite,
            valueMap,
            indexDefinition,
          );
          const indexMapHash = await indexMap.flush();
          const indexRecord: IndexRecord = {
            definition: nameIndexDefinition(name, indexDefinition),
            valueHash: indexMapHash,
          };
          indexes.set(name, new IndexWrite(indexRecord, indexMap));
        }
      } else {
        indexes = readIndexesForWrite(chain[chain.length - 1], dagWrite);
      }
      w = await newWriteSnapshotDD31(
        whenceHead(headName),
        lastMutationIDs ?? {
          [clientID]: await chain[chain.length - 1].getNextMutationID(
            clientID,
            dagWrite,
          ),
        },
        toInternalValue(cookie, ToInternalValueReason.Test),
        dagWrite,
        indexes,
        clientID,
      );
    } else {
      w = await newWriteSnapshot(
        whenceHead(DEFAULT_HEAD_NAME),
        await chain[chain.length - 1].getNextMutationID(clientID, dagWrite),
        toInternalValue(cookie, ToInternalValueReason.Test),
        dagWrite,
        readIndexesForWrite(chain[chain.length - 1], dagWrite),
        clientID,
      );
    }

    if (map) {
      for (const [k, v] of map) {
        await w.put(lc, k, toInternalValue(v, ToInternalValueReason.Test));
      }
    }
    await w.commit(headName);
  });
  return store.withRead(async dagRead => {
    const [, commit] = await readCommit(whenceHead(headName), dagRead);
    chain.push(commit);
    return chain;
  });
}

export class ChainBuilder {
  store: dag.Store;
  headName: string;
  chain: Chain;

  constructor(store: dag.Store, headName = DEFAULT_HEAD_NAME) {
    assert(DD31);
    this.store = store;
    this.headName = headName;
    this.chain = [];
  }

  async addGenesis(clientID: ClientID): Promise<Commit<SnapshotMetaDD31>> {
    await addGenesis(this.chain, this.store, clientID, this.headName);
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    assertSnapshotCommitDD31(commit);
    return commit;
  }

  async addLocal(
    clientID: ClientID,
    entries?: [string, JSONValue][],
  ): Promise<Commit<LocalMetaDD31>> {
    await addLocal(this.chain, this.store, clientID, entries, this.headName);
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    assertLocalCommitDD31(commit);
    return commit;
  }

  async addSnapshot(
    map: [string, JSONValue][] | undefined,
    clientID: ClientID,
    cookie: JSONValue = `cookie_${this.chain.length}`,
    lastMutationIDs?: Record<ClientID, number>,
    indexDefinitions?: IndexDefinitions,
  ): Promise<Commit<SnapshotMetaDD31>> {
    assert(DD31);
    await addSnapshot(
      this.chain,
      this.store,
      map,
      clientID,
      cookie,
      lastMutationIDs,
      indexDefinitions,
      this.headName,
    );
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    assertSnapshotCommitDD31(commit);
    return commit;
  }

  async removeHead(): Promise<void> {
    await this.store.withWrite(async write => {
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
  indexDefinitions: IndexDefinitions = {},
): Promise<Hash> {
  const basisHash = emptyHash;
  const indexes = await createEmptyIndexMaps(indexDefinitions, dagWrite);
  if (DD31) {
    const w = new Write(
      dagWrite,
      new BTreeWrite(dagWrite),
      undefined,
      {
        basisHash,
        type: MetaType.Snapshot,
        lastMutationIDs: {},
        cookieJSON: null,
      },
      indexes,
      clientID,
    );
    return await w.commit(headName);
  }
  const w = new Write(
    dagWrite,
    new BTreeWrite(dagWrite),
    undefined,
    {basisHash, type: MetaType.Snapshot, lastMutationID: 0, cookieJSON: null},
    indexes,
    clientID,
  );
  return await w.commit(headName);
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
      definition: nameIndexDefinition(name, indexDefinition),
      valueHash: emptyTreeHash,
    };
    indexes.set(
      name,
      new IndexWrite(indexRecord, new BTreeWrite(dagWrite, emptyTreeHash)),
    );
  }
  return indexes;
}

export class ChunkSnapshotVisitor extends Visitor {
  snapshot: Record<string, unknown> = {};

  override visitCommitChunk(chunk: dag.Chunk<CommitData<Meta>>): Promise<void> {
    this.snapshot[chunk.hash.toString()] = chunk.data;
    return super.visitCommitChunk(chunk);
  }

  override visitBTreeNodeChunk(chunk: dag.Chunk<Node>): Promise<void> {
    this.snapshot[chunk.hash.toString()] = chunk.data;
    return super.visitBTreeNodeChunk(chunk);
  }
}

export async function getChunkSnapshot(
  dagStore: dag.Store,
  hash: Hash,
): Promise<Record<string, unknown>> {
  return dagStore.withRead(async dagRead => {
    const v = new ChunkSnapshotVisitor(dagRead);
    await v.visitCommit(hash);
    return v.snapshot;
  });
}
