export const h32 = (s: string) => {
  digest(s, 1);
  return digests[0];
};
export const h64 = (s: string) => wide(s, 2);
export const h128 = (s: string) => wide(s, 4);

const PRIME32_1 = 2654435761;
const PRIME32_2 = 2246822519;
const PRIME32_3 = 3266489917;
const PRIME32_4 = 668265263;
const PRIME32_5 = 374761393;

const MAX_WORDS = 4;

// `digest` is synchronous and does not recurse, so these can be reused across
// calls instead of allocated per call.
const accs = new Int32Array(MAX_WORDS);
const stripes = new Int32Array(MAX_WORDS * 4);
const digests = new Uint32Array(MAX_WORDS);
let encoder: TextEncoder | undefined;

function round(acc: number, lane: number): number {
  const laneN0 = lane & 0xffff;
  const laneN1 = lane >>> 16;
  acc =
    (acc + (laneN0 * PRIME32_2 + ((laneN1 * PRIME32_2) << 16))) & 0xffffffff;
  acc = (acc << 13) | (acc >>> 19);
  return (
    ((acc & 0xffff) * PRIME32_1 + (((acc >>> 16) * PRIME32_1) << 16)) &
    0xffffffff
  );
}

/**
 * xxHash32 over the UTF-8 bytes of `str` under seeds `0..words-1`, leaving the
 * digests in `digests[0..words)`.
 *
 * A seed only changes the initial accumulator values, so every word digests the
 * same input lanes. This walks the bytes once, advancing all `words` sets of
 * accumulators per lane, and UTF-8 encodes the string once. The result is
 * identical to `words` separate `xxHash32(str, i)` calls — which is what this
 * used to delegate to, and what `hash.test.ts` checks it still matches.
 */
function digest(str: string, words: number): void {
  const b = (encoder ??= new TextEncoder()).encode(str);
  const len = b.length;

  for (let w = 0; w < words; w++) {
    accs[w] = (w + PRIME32_5) & 0xffffffff;
  }

  let offset = 0;

  /*
      Step 2. Process stripes
      A stripe is a contiguous segment of 16 bytes, evenly divided into 4 lanes
      of 4 bytes each, each lane updating its own accumulator. Inputs shorter
      than a stripe skip straight to step 4 with a single accumulator.
  */
  if (len >= 16) {
    for (let w = 0; w < words; w++) {
      const base = w << 2;
      stripes[base] = (w + PRIME32_1 + PRIME32_2) & 0xffffffff;
      stripes[base + 1] = (w + PRIME32_2) & 0xffffffff;
      stripes[base + 2] = w;
      stripes[base + 3] = (w - PRIME32_1) & 0xffffffff;
    }

    const limit = len - 16;
    for (; offset <= limit; offset += 16) {
      const l0 =
        b[offset] |
        (b[offset + 1] << 8) |
        (b[offset + 2] << 16) |
        (b[offset + 3] << 24);
      const l1 =
        b[offset + 4] |
        (b[offset + 5] << 8) |
        (b[offset + 6] << 16) |
        (b[offset + 7] << 24);
      const l2 =
        b[offset + 8] |
        (b[offset + 9] << 8) |
        (b[offset + 10] << 16) |
        (b[offset + 11] << 24);
      const l3 =
        b[offset + 12] |
        (b[offset + 13] << 8) |
        (b[offset + 14] << 16) |
        (b[offset + 15] << 24);
      for (let w = 0; w < words; w++) {
        const base = w << 2;
        stripes[base] = round(stripes[base], l0);
        stripes[base + 1] = round(stripes[base + 1], l1);
        stripes[base + 2] = round(stripes[base + 2], l2);
        stripes[base + 3] = round(stripes[base + 3], l3);
      }
    }

    /*
        Step 3. Accumulator convergence
        acc = (acc1 <<< 1) + (acc2 <<< 7) + (acc3 <<< 12) + (acc4 <<< 18);
    */
    for (let w = 0; w < words; w++) {
      const base = w << 2;
      const s0 = stripes[base];
      const s1 = stripes[base + 1];
      const s2 = stripes[base + 2];
      const s3 = stripes[base + 3];
      accs[w] =
        (((s0 << 1) | (s0 >>> 31)) +
          ((s1 << 7) | (s1 >>> 25)) +
          ((s2 << 12) | (s2 >>> 20)) +
          ((s3 << 18) | (s3 >>> 14))) &
        0xffffffff;
    }
  }

  // Step 4. Add input length.
  for (let w = 0; w < words; w++) {
    accs[w] = (accs[w] + len) & 0xffffffff;
  }

  // Step 5. Consume the remaining input, 4 bytes at a time and then 1 at a time.
  const limit4 = len - 4;
  for (; offset <= limit4; offset += 4) {
    const laneN0 = b[offset] + (b[offset + 1] << 8);
    const laneN1 = b[offset + 2] + (b[offset + 3] << 8);
    const laneP = laneN0 * PRIME32_3 + ((laneN1 * PRIME32_3) << 16);
    for (let w = 0; w < words; w++) {
      let acc = (accs[w] + laneP) & 0xffffffff;
      acc = (acc << 17) | (acc >>> 15);
      accs[w] =
        ((acc & 0xffff) * PRIME32_4 + (((acc >>> 16) * PRIME32_4) << 16)) &
        0xffffffff;
    }
  }

  for (; offset < len; ++offset) {
    const lane = b[offset];
    for (let w = 0; w < words; w++) {
      let acc = accs[w] + lane * PRIME32_5;
      acc = (acc << 11) | (acc >>> 21);
      accs[w] =
        ((acc & 0xffff) * PRIME32_1 + (((acc >>> 16) * PRIME32_1) << 16)) &
        0xffffffff;
    }
  }

  // Step 6. Final mix (avalanche). The Uint32Array store turns any negatives
  // back into a positive number.
  for (let w = 0; w < words; w++) {
    let acc = accs[w];
    acc = acc ^ (acc >>> 15);
    acc =
      (((acc & 0xffff) * PRIME32_2) & 0xffffffff) +
      (((acc >>> 16) * PRIME32_2) << 16);
    acc = acc ^ (acc >>> 13);
    acc =
      (((acc & 0xffff) * PRIME32_3) & 0xffffffff) +
      (((acc >>> 16) * PRIME32_3) << 16);
    digests[w] = acc ^ (acc >>> 16);
  }
}

/**
 * A hash wider than 32 bits, folding `words` digests together highest seed
 * first.
 */
function wide(str: string, words: number): bigint {
  digest(str, words);
  let result = 0n;
  for (let w = 0; w < words; w++) {
    result = (result << 32n) + BigInt(digests[w]);
  }
  return result;
}
