import {expect} from '@esm-bundle/chai';
import {LogContext} from '@rocicorp/logger';
import {assert, assertNotUndefined} from 'shared/asserts.js';
import * as btree from '../btree/mod.js';
import {BTreeWrite} from '../btree/mod.js';
import type {Cookie} from '../cookies.js';
import type * as dag from '../dag/mod.js';
import {Visitor} from '../dag/visitor.js';
import {FormatVersion} from '../format-version.js';
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
  formatVersion: FormatVersion,
): Promise<Chain> {
  expect(chain).to.have.length(0);
  const commit = await createGenesis(
    store,
    clientID,
    headName,
    indexDefinitions,
    formatVersion,
  );
  chain.push(commit);
  return chain;
}

async function createGenesis(
  store: dag.Store,
  clientID: ClientID,
  headName: string,
  indexDefinitions: IndexDefinitions,
  formatVersion: FormatVersion,
): Promise<Commit<Meta>> {
  await withWrite(store, async w => {
    await initDB(w, headName, clientID, indexDefinitions, formatVersion);
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
  formatVersion: FormatVersion,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const i = chain.length;
  const commit = await createLocal(
    entries ?? [[`local`, `${i}`]],
    store,
    i,
    clientID,
    headName,
    formatVersion,
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
  formatVersion: FormatVersion,
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
      formatVersion,
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
  formatVersion: FormatVersion,
): Promise<Chain> {
  assert(formatVersion <= FormatVersion.SDD);
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
    formatVersion,
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
  formatVersion: FormatVersion,
): Promise<Commit<Meta>> {
  assert(formatVersion <= FormatVersion.SDD);
  const lc = new LogContext();
  await withWrite(store, async dagWrite => {
    const w = await newWriteIndexChange(
      whenceHead(headName),
      dagWrite,
      clientID,
      formatVersion,
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
      formatVersion,
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
  formatVersion: FormatVersion,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const lc = new LogContext();
  await withWrite(store, async dagWrite => {
    let w;
    if (formatVersion >= FormatVersion.DD31) {
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
        formatVersion,
      );
    } else {
      w = await newWriteSnapshotSDD(
        whenceHead(DEFAULT_HEAD_NAME),
        await chain[chain.length - 1].getNextMutationID(clientID, dagWrite),
        deepFreeze(cookie),
        dagWrite,
        readIndexesForWrite(chain[chain.length - 1], dagWrite, formatVersion),
        clientID,
        formatVersion,
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
  readonly formatVersion: FormatVersion;

  constructor(
    store: dag.Store,
    headName = DEFAULT_HEAD_NAME,
    formatVersion: FormatVersion = FormatVersion.Latest,
  ) {
    this.store = store;
    this.headName = headName;
    this.chain = [];
    this.formatVersion = formatVersion;
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
      this.formatVersion,
    );
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    if (this.formatVersion >= FormatVersion.DD31) {
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
      this.formatVersion,
    );
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    if (this.formatVersion >= FormatVersion.DD31) {
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
      this.formatVersion,
    );
    const commit = this.chain.at(-1);
    assertNotUndefined(commit);
    if (this.formatVersion >= FormatVersion.DD31) {
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
    assert(this.formatVersion <= FormatVersion.SDD);
    await addIndexChange(
      this.chain,
      this.store,
      clientID,
      indexName,
      indexDefinition,
      this.headName,
      this.formatVersion,
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
      this.formatVersion,
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
  formatVersion: FormatVersion,
): Promise<Hash> {
  const basisHash = emptyHash;
  const indexes = await createEmptyIndexMaps(
    indexDefinitions,
    dagWrite,
    formatVersion,
  );
  const meta =
    formatVersion >= FormatVersion.DD31
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
    new BTreeWrite(dagWrite, formatVersion),
    undefined,
    meta,
    indexes,
    clientID,
    // TODO(arv): Pass format here too
    formatVersion,
  );
  return w.commit(headName);
}

async function createEmptyIndexMaps(
  indexDefinitions: IndexDefinitions,
  dagWrite: dag.Write,
  formatVersion: FormatVersion,
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
      new IndexWrite(
        indexRecord,
        new BTreeWrite(dagWrite, formatVersion, emptyTreeHash),
      ),
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
  formatVersion: FormatVersion,
): Promise<Write> {
  assert(formatVersion <= FormatVersion.SDD);
  const [basisHash, basis, bTreeWrite] = await readCommitForBTreeWrite(
    whence,
    dagWrite,
    formatVersion,
  );
  const lastMutationID = await basis.getMutationID(clientID, dagWrite);
  const indexes = readIndexesForWrite(basis, dagWrite, formatVersion);
  return new Write(
    dagWrite,
    bTreeWrite,
    basis,
    {basisHash, type: MetaType.IndexChangeSDD, lastMutationID},
    indexes,
    clientID,
    formatVersion,
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
  formatVersion: FormatVersion,
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
    formatVersion,
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
