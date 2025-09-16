/* eslint-disable @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/require-await, @typescript-eslint/unbound-method, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-argument, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/no-base-to-string, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/naming-convention, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, require-await, no-unused-private-class-members */
import type {Hash} from '../hash.ts';
import type {Chunk} from './chunk.ts';
import type {MustGetChunk} from './store.ts';

/**
 * A visitor walks the DAG starting at a given root and visits each chunk.
 */
export class Visitor {
  #seen: Set<Hash> = new Set();
  #dagRead: MustGetChunk;

  constructor(dagRead: MustGetChunk) {
    this.#dagRead = dagRead;
  }

  async visit(h: Hash) {
    if (this.#seen.has(h)) {
      return;
    }
    this.#seen.add(h);
    const chunk = await this.#dagRead.mustGetChunk(h);
    await this.visitChunk(chunk);
  }

  async visitChunk(chunk: Chunk<unknown>) {
    await Promise.all(chunk.meta.map(ref => this.visit(ref)));
  }
}
