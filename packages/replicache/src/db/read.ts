import {BTreeRead, BTreeWrite} from '../btree/mod.js';
import type * as dag from '../dag/mod.js';
import type {FormatVersion} from '../format-version.js';
import type {Hash} from '../hash.js';
import type {FrozenJSONValue} from '../json.js';
import {
  Commit,
  DEFAULT_HEAD_NAME,
  Meta,
  fromHash as commitFromHash,
} from './commit.js';
import {IndexRead} from './index.js';

export class Read {
  private readonly _dagRead: dag.Read;
  map: BTreeRead;
  readonly indexes: Map<string, IndexRead>;

  constructor(
    dagRead: dag.Read,
    map: BTreeRead,
    indexes: Map<string, IndexRead>,
  ) {
    this._dagRead = dagRead;
    this.map = map;
    this.indexes = indexes;
  }

  has(key: string): Promise<boolean> {
    return this.map.has(key);
  }

  get(key: string): Promise<FrozenJSONValue | undefined> {
    return this.map.get(key);
  }

  isEmpty(): Promise<boolean> {
    return this.map.isEmpty();
  }

  getMapForIndex(indexName: string): BTreeRead {
    const idx = this.indexes.get(indexName);
    if (idx === undefined) {
      throw new Error(`Unknown index name: ${indexName}`);
    }
    return idx.map;
  }

  get closed(): boolean {
    return this._dagRead.closed;
  }

  close(): void {
    this._dagRead.release();
  }
}

const enum WhenceType {
  Head,
  Hash,
}

export type Whence =
  | {
      type: WhenceType.Hash;
      hash: Hash;
    }
  | {
      type: WhenceType.Head;
      name: string;
    };

export function whenceHead(name: string): Whence {
  return {
    type: WhenceType.Head,
    name,
  };
}

export function whenceHash(hash: Hash): Whence {
  return {
    type: WhenceType.Hash,
    hash,
  };
}

export function readFromDefaultHead(
  dagRead: dag.Read,
  formatVersion: FormatVersion,
): Promise<Read> {
  return fromWhence(whenceHead(DEFAULT_HEAD_NAME), dagRead, formatVersion);
}

export async function fromWhence(
  whence: Whence,
  dagRead: dag.Read,
  formatVersion: FormatVersion,
): Promise<Read> {
  const [, basis, map] = await readCommitForBTreeRead(
    whence,
    dagRead,
    formatVersion,
  );
  const indexes = readIndexesForRead(basis, dagRead, formatVersion);
  return new Read(dagRead, map, indexes);
}

export async function readCommit(
  whence: Whence,
  dagRead: dag.Read,
): Promise<[Hash, Commit<Meta>]> {
  let hash: Hash;
  switch (whence.type) {
    case WhenceType.Hash:
      hash = whence.hash;
      break;
    case WhenceType.Head: {
      const h = await dagRead.getHead(whence.name);
      if (h === undefined) {
        throw new Error(`Unknown head: ${whence.name}`);
      }
      hash = h;
      break;
    }
  }

  const commit = await commitFromHash(hash, dagRead);
  return [hash, commit];
}

// TODO(arv): It looks like never read index 0 of the result.
export async function readCommitForBTreeRead(
  whence: Whence,
  dagRead: dag.Read,
  formatVersion: FormatVersion,
): Promise<[Hash, Commit<Meta>, BTreeRead]> {
  const [hash, commit] = await readCommit(whence, dagRead);
  return [
    hash,
    commit,
    new BTreeRead(dagRead, formatVersion, commit.valueHash),
  ];
}

export async function readCommitForBTreeWrite(
  whence: Whence,
  dagWrite: dag.Write,
  formatVersion: FormatVersion,
): Promise<[Hash, Commit<Meta>, BTreeWrite]> {
  const [hash, commit] = await readCommit(whence, dagWrite);
  return [
    hash,
    commit,
    new BTreeWrite(dagWrite, formatVersion, commit.valueHash),
  ];
}

export function readIndexesForRead(
  commit: Commit<Meta>,
  dagRead: dag.Read,
  formatVersion: FormatVersion,
): Map<string, IndexRead> {
  const m = new Map();
  for (const index of commit.indexes) {
    m.set(
      index.definition.name,
      new IndexRead(
        index,
        new BTreeRead(dagRead, formatVersion, index.valueHash),
      ),
    );
  }
  return m;
}
