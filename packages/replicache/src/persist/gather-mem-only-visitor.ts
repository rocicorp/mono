/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import {promiseVoid} from '../../../shared/src/resolved-promises.ts';
import type {Chunk} from '../dag/chunk.ts';
import type {LazyRead} from '../dag/lazy-store.ts';
import {Visitor} from '../dag/visitor.ts';
import type {Hash} from '../hash.ts';

export class GatherMemoryOnlyVisitor extends Visitor {
  readonly #gatheredChunks: Map<Hash, Chunk> = new Map();
  readonly #lazyRead: LazyRead;

  constructor(dagRead: LazyRead) {
    super(dagRead);
    this.#lazyRead = dagRead;
  }

  get gatheredChunks(): ReadonlyMap<Hash, Chunk> {
    return this.#gatheredChunks;
  }

  override visit(h: Hash): Promise<void> {
    if (!this.#lazyRead.isMemOnlyChunkHash(h)) {
      // Not a memory-only hash, no need to visit anything else.
      return promiseVoid;
    }
    return super.visit(h);
  }

  override visitChunk(chunk: Chunk): Promise<void> {
    this.#gatheredChunks.set(chunk.hash, chunk);
    return super.visitChunk(chunk);
  }
}
