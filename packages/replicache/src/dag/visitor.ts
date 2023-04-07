import type {Hash} from '../hash.js';
import type {Chunk} from './chunk.js';
import type {MustGetChunk} from './store.js';

/**
 * A visitor walks the DAG starting at a given root and visits each chunk.
 */
export class Visitor {
  private _seen: Set<Hash> = new Set();
  private _dagRead: MustGetChunk;

  constructor(dagRead: MustGetChunk) {
    this._dagRead = dagRead;
  }

  async visit(h: Hash) {
    if (this._seen.has(h)) {
      return;
    }
    this._seen.add(h);
    const chunk = await this._dagRead.mustGetChunk(h);
    await this.visitChunk(chunk);
  }

  async visitChunk(chunk: Chunk<unknown>) {
    await Promise.all(chunk.meta.map(ref => this.visit(ref)));
  }
}
