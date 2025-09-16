/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import type {Hash} from '../hash.ts';
import type {Chunk} from './chunk.ts';
import {LazyStore} from './lazy-store.ts';

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
