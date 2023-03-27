import type {Hash} from '../hash.js';
import type {Chunk, Refs} from './chunk.js';
import {LazyStore} from './lazy-store.js';

export class TestLazyStore extends LazyStore {
  getRefCountsSnapshot(): Record<Hash, number> {
    return Object.fromEntries(this._refCounts);
  }

  getMemOnlyChunksSnapshot(): Record<Hash, Chunk> {
    return Object.fromEntries(this._memOnlyChunks);
  }

  getRefsSnapshot(): Record<Hash, Refs> {
    return Object.fromEntries(this._refs);
  }

  getCachedSourceChunksSnapshot(): Refs {
    return new Set(this._sourceChunksCache.cacheEntries.keys());
  }
}
