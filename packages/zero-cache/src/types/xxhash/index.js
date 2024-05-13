// The .wasm is filled in by the build process, so the user doesn't need to load
// xxhash.wasm by themselves because it's part of the bundle. Otherwise it
// couldn't be distributed easily as the user would need to host xxhash.wasm
// and then fetch it, to be able to use it.
// eslint-disable-next-line no-undef
// const wasmBytes = new Uint8Array(WASM_PRECOMPILED_BYTES);

/* eslint-disable */

// Cloudflare requires wasm modules to be import to prevent dynamic code execution.
import wasmModule from './xxhash.wasm';

const u32_BYTES = 4;
const u64_BYTES = 8;

// The xxh32 hash state struct:
const XXH32_STATE_SIZE_BYTES =
  u32_BYTES + // total_len
  u32_BYTES + // large_len
  u32_BYTES * 4 + // Accumulator lanes
  u32_BYTES * 4 + // Internal buffer
  u32_BYTES + // memsize
  u32_BYTES; // reserved

// The xxh64 hash state struct:
const XXH64_STATE_SIZE_BYTES =
  u64_BYTES + // total_len
  u64_BYTES * 4 + // Accumulator lanes
  u64_BYTES * 4 + // Internal buffer
  u32_BYTES + // memsize
  u32_BYTES + // reserved32
  u64_BYTES; // reserved64

async function xxhash() {
  const {
    // Cloudflare wasmModule is already a WebAssembly.Module
    exports: {
      mem,
      xxh32,
      xxh64,
      init32,
      update32,
      digest32,
      init64,
      update64,
      digest64,
    },
  } = await WebAssembly.instantiate(wasmModule);

  let memory = new Uint8Array(mem.buffer);
  // Grow the wasm linear memory to accommodate length + offset bytes
  function growMemory(length, offset) {
    if (mem.buffer.byteLength < length + offset) {
      const extraPages = Math.ceil(
        // Wasm pages are spec'd to 64K
        (length + offset - mem.buffer.byteLength) / (64 * 1024),
      );
      mem.grow(extraPages);
      // After growing, the original memory's ArrayBuffer is detached, so we'll
      // need to replace our view over it with a new one over the new backing
      // ArrayBuffer.
      memory = new Uint8Array(mem.buffer);
    }
  }

  // The h32 and h64 streaming hash APIs are identical, so we can implement
  // them both by way of a templated call to this generalized function.
  function create(size, seed, init, update, digest, finalize) {
    // Ensure that we've actually got enough space in the wasm memory to store
    // the state blob for this hasher.
    growMemory(size);

    // We'll hold our hashing state in this closure.
    const state = new Uint8Array(size);
    memory.set(state);
    init(0, seed);

    // Each time we interact with wasm, it may have mutated our state so we'll
    // need to read it back into our closed copy.
    state.set(memory.slice(0, size));

    return {
      update(input) {
        memory.set(state);
        let length;
        if (typeof input === 'string') {
          growMemory(input.length * 3, size);
          length = encoder.encodeInto(input, memory.subarray(size)).written;
        } else {
          // The only other valid input type is a Uint8Array
          growMemory(input.byteLength, size);
          memory.set(input, size);
          length = input.byteLength;
        }
        update(0, size, length);
        state.set(memory.slice(0, size));
        return this;
      },
      digest() {
        memory.set(state);
        return finalize(digest(0));
      },
    };
  }

  // Logical shift right makes it an u32, otherwise it's interpreted as an i32.
  function forceUnsigned32(i) {
    return i >>> 0;
  }

  // BigInts are arbitrary precision and signed, so to get the "correct" u64
  // value from the return, we'll need to force that interpretation.
  const u64Max = 2n ** 64n - 1n;
  function forceUnsigned64(i) {
    return i & u64Max;
  }

  const encoder = new TextEncoder();
  const defaultSeed = 0;
  const defaultBigSeed = 0n;

  function h32(str, seed = defaultSeed) {
    // https://developer.mozilla.org/en-US/docs/Web/API/TextEncoder/encodeInto#buffer_sizing
    // By sizing the buffer to 3 * string-length we guarantee that the buffer
    // will be appropriately sized for the utf-8 encoding of the string.
    growMemory(str.length * 3, 0);
    return forceUnsigned32(
      xxh32(0, encoder.encodeInto(str, memory).written, seed),
    );
  }

  function h64(str, seed = defaultBigSeed) {
    growMemory(str.length * 3, 0);
    return forceUnsigned64(
      xxh64(0, encoder.encodeInto(str, memory).written, seed),
    );
  }

  return {
    h32,
    h32ToString(str, seed = defaultSeed) {
      return h32(str, seed).toString(16).padStart(8, '0');
    },
    h32Raw(inputBuffer, seed = defaultSeed) {
      growMemory(inputBuffer.byteLength, 0);
      memory.set(inputBuffer);
      return forceUnsigned32(xxh32(0, inputBuffer.byteLength, seed));
    },
    create32(seed = defaultSeed) {
      return create(
        XXH32_STATE_SIZE_BYTES,
        seed,
        init32,
        update32,
        digest32,
        forceUnsigned32,
      );
    },
    h64,
    h64ToString(str, seed = defaultBigSeed) {
      return h64(str, seed).toString(16).padStart(16, '0');
    },
    h64Raw(inputBuffer, seed = defaultBigSeed) {
      growMemory(inputBuffer.byteLength, 0);
      memory.set(inputBuffer);
      return forceUnsigned64(xxh64(0, inputBuffer.byteLength, seed));
    },
    create64(seed = defaultBigSeed) {
      return create(
        XXH64_STATE_SIZE_BYTES,
        seed,
        init64,
        update64,
        digest64,
        forceUnsigned64,
      );
    },
  };
}

export default xxhash;

/* eslint-enable */
