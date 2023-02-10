import {assert} from '../asserts';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import type {Hash} from '../hash';
import type {MaybePromise} from '../replicache';

export type GatheredChunks = ReadonlyMap<Hash, dag.Chunk>;
export type FixedChunks = ReadonlyMap<Hash, dag.Chunk>;

/**
 * This transformer computes the hashes
 */
export class ComputeHashTransformer extends db.BaseTransformer {
  private readonly _fixedChunks: Map<Hash, dag.Chunk> = new Map();
  private readonly _gatheredChunks: GatheredChunks;
  private readonly _hashFunc: <V>(value: V) => MaybePromise<Hash>;

  /**
   * @param dagWrite The destination dag.
   * @param gatheredChunks The chunks that were gathered on the source dag in
   * a previous pass.
   */
  constructor(
    gatheredChunks: GatheredChunks,
    hashFunc: <V>(value: V) => MaybePromise<Hash>,
  ) {
    super();
    this._gatheredChunks = gatheredChunks;
    this._hashFunc = hashFunc;
  }

  get fixedChunks(): FixedChunks {
    return this._fixedChunks;
  }

  override shouldSkip(oldHash: Hash): boolean {
    // Skip all chunks that we did not get from the source.
    return !this._gatheredChunks.has(oldHash);
  }

  protected override shouldForceWrite(oldHash: Hash): boolean {
    // We want to write the chunk to the destination dag even if the chunk did
    // not change because the computed hash on the source is different than the
    // one computed on the destination.
    return this._gatheredChunks.has(oldHash);
  }

  override async getChunk(hash: Hash): Promise<dag.Chunk | undefined> {
    const gatheredChunk = this._gatheredChunks.get(hash);
    // We cannot get here if we did not gather a chunk for this hash.
    assert(gatheredChunk !== undefined);
    return gatheredChunk;
  }

  protected override async writeChunk<D>(
    _h: Hash,
    data: D,
    getRefs: (data: D) => readonly Hash[],
  ): Promise<Hash> {
    const hash = await this._hashFunc(data);
    const chunk = dag.createChunkWithHash(hash, data, getRefs(data));
    this._fixedChunks.set(hash, chunk);
    return hash;
  }
}
