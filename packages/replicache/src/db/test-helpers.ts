import {LogContext} from '@rocicorp/logger';
import {expect} from 'vitest';
import {assert, assertNotUndefined} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {emptyDataNode} from '../btree/node.ts';
import {BTreeWrite} from '../btree/write.ts';
import type {Cookie} from '../cookies.ts';
import type {Chunk} from '../dag/chunk.ts';
import {
  type Write as DagWrite,
  type Store,
  mustGetHeadHash,
} from '../dag/store.ts';
import {Visitor} from '../dag/visitor.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze} from '../frozen-json.ts';
import {type Hash, emptyHash} from '../hash.ts';
import type {IndexDefinitions} from '../index-defs.ts';
import type {ClientID} from '../sync/ids.ts';
import {addSyncSnapshot} from '../sync/test-helpers.ts';
import {
  withRead,
  withWrite,
  withWriteNoImplicitCommit,
} from '../with-transactions.ts';
import type {Commit} from './commit.ts';
import {
  DEFAULT_HEAD_NAME,
  type IndexRecord,
  type LocalMeta,
  type Meta,
  type SnapshotMetaDD31,
  assertLocalCommitDD31,
  assertSnapshotCommitDD31,
  commitFromHead,
  toChunkIndexDefinition,
} from './commit.ts';
import {IndexWrite} from './index.ts';
import * as MetaType from './meta-type-enum.ts';
import {Write, newWriteLocal, newWriteSnapshotDD31} from './write.ts';

type FormatVersion = Enum<typeof FormatVersion>;

export type Chain = Commit<Meta>[];

async function addGenesis(
  chain: Chain,
  store: Store,
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
  store: Store,
  clientID: ClientID,
  headName: string,
  indexDefinitions: IndexDefinitions,
  formatVersion: FormatVersion,
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
  store: Store,
  i: number,
  clientID: ClientID,
  headName: string,
  formatVersion: FormatVersion,
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
  formatVersion: FormatVersion,
): Promise<Chain> {
  expect(chain).to.have.length.greaterThan(0);
  const lc = new LogContext();
  await withWriteNoImplicitCommit(store, async dagWrite => {
    assert(formatVersion >= FormatVersion.DD31);
    const w = await newWriteSnapshotDD31(
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
  readonly formatVersion: FormatVersion;

  constructor(
    store: Store,
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
  ): Promise<Commit<SnapshotMetaDD31>> {
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
    assert(this.formatVersion >= FormatVersion.DD31);
    assertSnapshotCommitDD31(commit);
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
    assert(this.formatVersion >= FormatVersion.DD31);
    assertLocalCommitDD31(commit);

    return commit;
  }

  async addSnapshot(
    map: [string, JSONValue][] | undefined,
    clientID: ClientID,
    cookie: Cookie = `cookie_${this.chain.length}`,
    lastMutationIDs?: Record<ClientID, number>,
  ): Promise<Commit<SnapshotMetaDD31>> {
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
    assert(this.formatVersion >= FormatVersion.DD31);
    assertSnapshotCommitDD31(commit);
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
  formatVersion: FormatVersion,
): Promise<Hash> {
  const basisHash = emptyHash;
  const indexes = await createEmptyIndexMaps(
    indexDefinitions,
    dagWrite,
    formatVersion,
  );
  assert(formatVersion >= FormatVersion.DD31);
  const meta = {
    basisHash,
    type: MetaType.SnapshotDD31,
    lastMutationIDs: {},
    cookieJSON: null,
  } as const;

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
  formatVersion: FormatVersion,
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
