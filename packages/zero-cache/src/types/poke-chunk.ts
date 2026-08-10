import {assert} from '../../../shared/src/asserts.ts';
import type {PokeChunk} from '../../../zero-protocol/src/poke.ts';

export const POKE_CHUNK_BYTES = 1024 * 1024;
export const POKE_CHUNK_PROTOCOL_VERSION = 52;

export type EmitPokeChunk = (chunk: PokeChunk) => Promise<void>;

/**
 * Streams a JSON array into fixed-size UTF-8 chunks.
 *
 * The emitter must finish consuming a chunk before its promise resolves. This
 * lets the encoder reuse one fixed-size buffer for the entire poke. Memory is
 * therefore O(chunk size), independent of poke size.
 */
export class PokeChunkEncoder {
  readonly #encoder = new TextEncoder();
  readonly #buffer: Uint8Array;
  #bufferedBytes = 0;
  #patchCount = 0;
  #finished = false;

  constructor(chunkBytes = POKE_CHUNK_BYTES) {
    // Four bytes are required to encode the largest UTF-8 code point. A
    // smaller buffer could make encodeInto() unable to make progress.
    assert(chunkBytes >= 4, 'poke chunk size must be at least four bytes');
    this.#buffer = new Uint8Array(chunkBytes);
  }

  async addPatch(serializedPatch: string, emit: EmitPokeChunk): Promise<void> {
    assert(!this.#finished, 'cannot add a patch to a finished poke');
    await this.#write(this.#patchCount++ === 0 ? '[' : ',', emit);
    await this.#write(serializedPatch, emit);
  }

  async finish(emit: EmitPokeChunk): Promise<void> {
    assert(!this.#finished, 'cannot finish a poke twice');
    this.#finished = true;
    await this.#write(this.#patchCount === 0 ? '[]' : ']', emit);
    await this.#flush(emit);
  }

  cancel(): void {
    this.#finished = true;
    this.#bufferedBytes = 0;
  }

  async #write(value: string, emit: EmitPokeChunk): Promise<void> {
    let read = 0;
    while (read < value.length) {
      const target = this.#buffer.subarray(this.#bufferedBytes);
      const result = this.#encoder.encodeInto(value.slice(read), target);
      read += result.read;
      this.#bufferedBytes += result.written;

      // A multi-byte code point may not fit in the remaining space.
      if (this.#bufferedBytes === this.#buffer.length || result.read === 0) {
        await this.#flush(emit);
      }
    }
  }

  async #flush(emit: EmitPokeChunk): Promise<void> {
    if (this.#bufferedBytes === 0) {
      return;
    }
    await emit(this.#buffer.subarray(0, this.#bufferedBytes));
    this.#bufferedBytes = 0;
  }
}
