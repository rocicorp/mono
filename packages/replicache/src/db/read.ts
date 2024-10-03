import {BTreeRead} from '../btree/read.js';
import type {Read as DagRead} from '../dag/store.js';
import type {FormatVersion} from '../format-version-enum.js';
import type {FrozenJSONValue} from '../frozen-json.js';
import type {Hash} from '../hash.js';
import {
  Commit,
  DEFAULT_HEAD_NAME,
  type Meta,
  commitFromHash,
  commitFromHead,
} from './commit.js';
import {IndexRead} from './index.js';

export class Read {
  readonly #dagRead: DagRead;
  map: BTreeRead;
  readonly indexes: Map<string, IndexRead>;

  constructor(
    dagRead: DagRead,
    map: BTreeRead,
    indexes: Map<string, IndexRead>,
  ) {
    this.#dagRead = dagRead;
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
    return this.#dagRead.closed;
  }

  close(): void {
    this.#dagRead.release();
  }
}

export function readFromDefaultHead(
  dagRead: DagRead,
  formatVersion: FormatVersion,
): Promise<Read> {
  return readFromHead(DEFAULT_HEAD_NAME, dagRead, formatVersion);
}

export async function readFromHead(
  name: string,
  dagRead: DagRead,
  formatVersion: FormatVersion,
): Promise<Read> {
  const commit = await commitFromHead(name, dagRead);
  return readFromCommit(commit, dagRead, formatVersion);
}

export async function readFromHash(
  hash: Hash,
  dagRead: DagRead,
  formatVersion: FormatVersion,
): Promise<Read> {
  const commit = await commitFromHash(hash, dagRead);
  return readFromCommit(commit, dagRead, formatVersion);
}

function readFromCommit(
  commit: Commit<Meta>,
  dagRead: DagRead,
  formatVersion: FormatVersion,
): Read {
  const indexes = readIndexesForRead(commit, dagRead, formatVersion);
  const map = new BTreeRead(dagRead, formatVersion, commit.valueHash);
  return new Read(dagRead, map, indexes);
}

export function readIndexesForRead(
  commit: Commit<Meta>,
  dagRead: DagRead,
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
