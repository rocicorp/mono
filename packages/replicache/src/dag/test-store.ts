import {assertArray, assertString} from 'shared/src/asserts.js';
import type {Hash} from '../hash.js';
import {
  assertHash,
  makeNewFakeHashFunction,
  parse as parseHash,
} from '../hash.js';
import {TestMemStore} from '../kv/test-mem-store.js';
import {stringCompare} from '../string-compare.js';
import {Chunk, ChunkHasher} from './chunk.js';
import {KeyType, chunkMetaKey, parse as parseKey} from './key.js';
import {StoreImpl} from './store-impl.js';

export class TestStore extends StoreImpl {
  readonly kvStore: TestMemStore;

  constructor(
    kvStore = new TestMemStore(),
    chunkHasher: ChunkHasher = makeNewFakeHashFunction(),
    assertValidHash = assertHash,
  ) {
    super(kvStore, chunkHasher, assertValidHash);
    this.kvStore = kvStore;
  }

  chunks(): Chunk[] {
    const rv: Chunk[] = [];
    for (const [key, value] of this.kvStore.entries()) {
      const pk = parseKey(key);
      if (pk.type === KeyType.ChunkData) {
        const refsValue = this.kvStore.map().get(chunkMetaKey(pk.hash));
        rv.push(new Chunk(pk.hash, value, toRefs(refsValue)));
      }
    }
    return sortByHash(rv);
  }

  chunkHashes(): Set<Hash> {
    const hashes = new Set<Hash>();
    for (const key of this.kvStore.map().keys()) {
      const pk = parseKey(key);
      if (pk.type === KeyType.ChunkData) {
        hashes.add(pk.hash);
      }
    }
    return hashes;
  }

  clear(): void {
    this.kvStore.clear();
  }
}

function sortByHash(arr: Iterable<Chunk>): Chunk[] {
  return [...arr].sort((a, b) => stringCompare(String(a.hash), String(b.hash)));
}

function toRefs(refs: unknown): Hash[] {
  if (refs === undefined) {
    return [];
  }
  assertArray(refs);
  return refs.map(h => {
    assertString(h);
    return parseHash(h);
  });
}
