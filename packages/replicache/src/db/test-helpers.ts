import {LogContext} from '@rocicorp/logger';
import {expect} from 'vitest';
import {assert, assertNotUndefined} from '../../../shared/src/asserts.js';
import type {JSONValue} from '../../../shared/src/json.js';
import {emptyDataNode} from '../btree/node.js';
import {BTreeWrite} from '../btree/write.js';
import type {Cookie} from '../cookies.js';
import type {Chunk} from '../dag/chunk.js';
import {
  type Write as DagWrite,
  type Store,
  mustGetHeadHash,
} from '../dag/store.js';
import {Visitor} from '../dag/visitor.js';
import * as FormatVersion from '../format-version-enum.js';
import {deepFreeze} from '../frozen-json.js';
import {type Hash, emptyHash} from '../hash.js';
import type {IndexDefinitions} from '../index-defs.js';
import type {ClientID} from '../sync/ids.js';
import {addSyncSnapshot} from '../sync/test-helpers.js';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.js';
import {
  Commit,
  DEFAULT_HEAD_NAME,
  type IndexRecord,
  type LocalMeta,
  type Meta,
  type SnapshotMetaDD31,
  type SnapshotMetaSDD,
  assertLocalCommitDD31,
  assertLocalCommitSDD,
  assertSnapshotCommitDD31,
  assertSnapshotCommitSDD,
  commitFromHead,
  toChunkIndexDefinition,
} from './commit.js';
import {IndexWrite} from './index.js';
import * as MetaType from './meta-type-enum.js';
import {
  Write,
  newWriteLocal,
  newWriteSnapshotDD31,
  newWriteSnapshotSDD,
  readIndexesForWrite,
} from './write.js';

export type Chain = Commit<Meta>[];

async function addGenesis(
  chain: Chain,
  store: Store,
  clientID: ClientID,
  headName = DEFAULT_HEAD_NAME,
  indexDefinitions: IndexDefinitions,
  formatVersion: FormatVersion.Type,
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
  store: Store,
  clientID: ClientID,
  headName: string,
  indexDefinitions: IndexDefinitions,
  formatVersion: FormatVersion.Type,
): Promise<Commit<Meta>> {
  await withWriteNoImplicitCommit(store, async w => {
    await initDB(w, headName, clientID, indexDefinitions, formatVersion);
  });
  return withRead(store, read => commitFromHead(headName, read));
}

// Local commit has mutator name and args according to its index in the
// chain.
async function addLocal(
  chain: Chain,
  store: Store,
  clientID: ClientID,
  entries: [string, JSONValue][] | undefined,
  headName: string,
  formatVersion: FormatVersion.Type,
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
  store: Store,
  i: number,
  clientID: ClientID,
  headName: string,
  formatVersion: FormatVersion.Type,
): Promise<Commit<Meta>> {
  const lc = new LogContext();
  await withWriteNoImplicitCommit(store, async dagWrite => {
    const w = await newWriteLocal(
      await mustGetHeadHash(headName, dagWrite),
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
  return withRead(store, dagRead => commitFromHead(headName, dagRead));
}

export function createMutatorName(chainIndex: number): string {
  return `mutator_name_${chainIndex}`;
}

// See also sync.test_helpers for addSyncSnapshot, which can't go here because
// it depends on details of sync and sync depends on db.

// The optional map for the commit is treated as key, value pairs.
async function addSnapshot(
  chain: Chain,
  store: Store,
  map: [string, JSONValue][] | undefined,
  clientID: ClientID,
  cookie: Cookie = `cookie_${chain.length}`,
  lastMutationIDs: Record<ClientID, number> | undefined,
  headName: string,
  formatVersion: FormatVersion.Type,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const lc = new LogContext();
  await withWriteNoImplicitCommit(store, async dagWrite => {
    let w;
    if (formatVersion >= FormatVersion.DD31) {
      w = await newWriteSnapshotDD31(
        await mustGetHeadHash(headName, dagWrite),
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
        await mustGetHeadHash(DEFAULT_HEAD_NAME, dagWrite),
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
    const commit = await commitFromHead(headName, dagRead);
    chain.push(commit);
    return chain;
  });
}

export class ChainBuilder {
  readonly store: Store;
  readonly headName: string;
  chain: Chain;
  readonly formatVersion: FormatVersion.Type;

  constructor(
    store: Store,
    headName = DEFAULT_HEAD_NAME,
    formatVersion: FormatVersion.Type = FormatVersion.Latest,
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
    });
  }

  get headHash(): Hash {
    const lastCommit = this.chain.at(-1);
    assert(lastCommit);
    return lastCommit.chunk.hash;
  }
}

export async function initDB(
  dagWrite: DagWrite,
  headName: string,
  clientID: ClientID,
  indexDefinitions: IndexDefinitions,
  formatVersion: FormatVersion.Type,
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
  dagWrite: DagWrite,
  formatVersion: FormatVersion.Type,
): Promise<Map<string, IndexWrite>> {
  const indexes = new Map();

  let emptyTreeHash: Hash | undefined;
  for (const [name, indexDefinition] of Object.entries(indexDefinitions)) {
    if (!emptyTreeHash) {
      const emptyBTreeChunk = dagWrite.createChunk(emptyDataNode, []);
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

  override visitChunk(chunk: Chunk): Promise<void> {
    this.snapshot[chunk.hash.toString()] = chunk.data;
    return super.visitChunk(chunk);
  }
}

export function getChunkSnapshot(
  dagStore: Store,
  hash: Hash,
): Promise<Record<string, unknown>> {
  return withRead(dagStore, async dagRead => {
    const v = new ChunkSnapshotVisitor(dagRead);
    await v.visit(hash);
    return v.snapshot;
  });
}
