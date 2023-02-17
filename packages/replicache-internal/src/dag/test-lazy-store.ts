import type {Hash} from '../hash.js';
import type {Chunk} from './chunk.js';
import {LazyStore} from './lazy-store.js';

export class TestLazyStore extends LazyStore {
  getRefCountsSnapshot(): Record<Hash, number> {
    return Object.fromEntries(this._refCounts);
  }

  getMemOnlyChunksSnapshot(): Record<Hash, Chunk> {
    return Object.fromEntries(this._memOnlyChunks);
  }

  getRefsSnapshot(): Record<Hash, readonly Hash[]> {
    return Object.fromEntries(this._refs);
  }

  getCachedSourceChunksSnapshot(): readonly Hash[] {
    return [...this._sourceChunksCache.cacheEntries.keys()];
  }
}
