import {assert} from '../../../shared/src/asserts.ts';
import {
  POKE_CHUNK_MESSAGE_TYPE,
  type PokeChunk,
} from '../../../zero-protocol/src/poke.ts';

const POKE_CHUNK_BYTES = 1024 * 1024;
const INITIAL_POKE_CHUNK_BYTES = 16 * 1024;

export type EmitPokeChunk = (chunk: PokeChunk) => Promise<void>;

/**
 * Streams a JSON array into bounded UTF-8 chunks.
 *
 * The emitter must finish consuming a chunk before its promise resolves. This
 * lets the encoder reuse its buffer for the rest of the poke, and lets the
 * owner reuse it for its next poke once finish() resolves. Memory is
 * therefore O(chunk size), independent of poke size.
 *
 * The buffer starts at `INITIAL_POKE_CHUNK_BYTES` and doubles up to
 * `chunkBytes` when a poke needs more, so frames are at most `chunkBytes`.
 */
export class PokeChunkEncoder {
  readonly #encoder = new TextEncoder();
  readonly #chunkBytes: number;
  #buffer: Uint8Array;
  #bufferedBytes = 1;
  #patchCount = 0;
  #finished = false;
  #grown = false;
  /** Set while addPatch() runs, and left set if it rejects. */
  #patchInProgress = false;

  /**
   * @param buffer The buffer returned by the previous poke's finish(), to
   *   reuse instead of allocating one.
   */
  constructor(buffer?: Uint8Array | undefined, chunkBytes = POKE_CHUNK_BYTES) {
    // One byte tags the binary message and four bytes are required to encode
    // the largest UTF-8 code point. A smaller buffer could prevent progress.
    assert(chunkBytes >= 5, 'poke chunk size must be at least five bytes');
    this.#chunkBytes = chunkBytes;
    this.#buffer =
      buffer ?? new Uint8Array(Math.min(INITIAL_POKE_CHUNK_BYTES, chunkBytes));
    this.#buffer[0] = POKE_CHUNK_MESSAGE_TYPE;
  }

  async addPatch(serializedPatch: string, emit: EmitPokeChunk): Promise<void> {
    assert(!this.#finished, 'cannot add a patch to a finished poke');
    assert(!this.#patchInProgress, 'previous addPatch() did not complete');
    this.#patchInProgress = true;
    await this.#write(this.#patchCount++ === 0 ? '[' : ',', emit);
    await this.#write(serializedPatch, emit);
    this.#patchInProgress = false;
  }

  /**
   * Returns the buffer once the last emit of its bytes has resolved, so the
   * owner can pass it to the next poke's encoder. Only this path hands the
   * buffer out: a cancelled poke, or one whose emit in finish() failed or timed
   * out, never returns it, since the socket may still be reading the chunk.
   *
   * finish() throws if an addPatch() rejected or hasn't resolved. Otherwise it
   * would send the failed chunk again, close the array in the middle of a
   * patch, and return a buffer the socket may still be reading.
   *
   * A buffer that grew is not returned, so an idle owner holds at most
   * `INITIAL_POKE_CHUNK_BYTES`.
   */
  async finish(emit: EmitPokeChunk): Promise<Uint8Array | undefined> {
    assert(!this.#finished, 'cannot finish a poke twice');
    assert(!this.#patchInProgress, 'previous addPatch() did not complete');
    this.#finished = true;
    await this.#write(this.#patchCount === 0 ? '[]' : ']', emit);
    await this.#flush(emit);
    return this.#grown ? undefined : this.#buffer;
  }

  cancel(): void {
    this.#finished = true;
    this.#bufferedBytes = 1;
  }

  async #write(value: string, emit: EmitPokeChunk): Promise<void> {
    let read = 0;
    while (read < value.length) {
      const target = this.#buffer.subarray(this.#bufferedBytes);
      const result = this.#encoder.encodeInto(value.slice(read), target);
      read += result.read;
      this.#bufferedBytes += result.written;

      // The rest didn't fit. The buffer may be full, or a multi-byte code
      // point may not fit in the remaining space.
      if (read < value.length) {
        if (this.#buffer.length < this.#chunkBytes) {
          this.#grow();
        } else {
          await this.#flush(emit);
        }
      }
    }
  }

  #grow(): void {
    const buffer = new Uint8Array(
      Math.min(this.#buffer.length * 2, this.#chunkBytes),
    );
    buffer.set(this.#buffer.subarray(0, this.#bufferedBytes));
    this.#buffer = buffer;
    this.#grown = true;
  }

  async #flush(emit: EmitPokeChunk): Promise<void> {
    if (this.#bufferedBytes === 1) {
      return;
    }
    await emit(this.#buffer.subarray(0, this.#bufferedBytes));
    this.#bufferedBytes = 1;
  }
}
