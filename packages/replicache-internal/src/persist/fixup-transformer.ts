import type * as kv from '../kv/mod';
import * as dag from '../dag/mod';
import * as db from '../db/mod';
import {Hash, isTempHash} from '../hash';

type OldHash = Hash;
type NewHash = Hash;
/**
 * Mappings are used to map from one hash to another. This is used when moving
 * chunks between two DAG stores that uses different hash functions.
 */
type Mappings = ReadonlyMap<OldHash, NewHash>;

/**
 * This transformer is used to fixup the hashes we got from the source dag (the
 * perdag in our case) and rewrite the required chunks in the destination dag
 * (the memdag).
 */
export class FixupTransformer extends db.Transformer {
  private readonly _mappings: Mappings;

  constructor(dagWrite: dag.Write, mappings: Mappings) {
    super(dagWrite);
    this._mappings = mappings;
  }

  override shouldSkip(hash: Hash): boolean {
    if (isTempHash(hash)) {
      return false;
    }
    return !this._mappings.has(hash);
  }

  override shouldForceWrite(h: OldHash): boolean {
    // If there is a mapping we need to write this chunk even if the data did
    // not change. This happens for BTree data nodes for example because the
    // hash it had in the memdag was using a temp hash but now we got a real
    // hash from the perdag.
    return this._mappings.has(h);
  }

  protected override async writeChunk<D extends kv.Value>(
    oldHash: OldHash,
    data: D,
    getRefs: (data: D) => readonly Hash[],
  ): Promise<Hash> {
    // We get here if the chunk changed or if we had a mapping for the hash.
    const newHash = this._mappings.get(oldHash);
    const refs = getRefs(data);
    const newChunk = newHash
      ? dag.createChunkWithHash(newHash, data, refs)
      : this.dagWrite.createChunk(data, refs);
    await this.dagWrite.putChunk(newChunk);
    return newChunk.hash;
  }
}
