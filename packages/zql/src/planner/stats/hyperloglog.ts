/**
 * HyperLogLog probabilistic cardinality estimator.
 *
 * This implementation uses precision p=14 (16384 registers) which provides
 * approximately 1.6% standard error with ~16KB memory per sketch.
 *
 * Based on the HyperLogLog algorithm described in:
 * "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm"
 * by Flajolet et al. (2007)
 */

const PRECISION = 14;
const NUM_REGISTERS = 1 << PRECISION; // 2^14 = 16384
const REGISTER_MASK = NUM_REGISTERS - 1; // 0x3FFF

/**
 * MurmurHash3 32-bit hash function for strings.
 * Provides good distribution for HyperLogLog.
 */
function murmurHash3(key: string, seed = 0): number {
  let h = seed;
  const len = key.length;

  // Process 4 bytes at a time
  for (let i = 0; i < len; i++) {
    let k = key.charCodeAt(i);
    k = Math.imul(k, 0xcc9e2d51);
    k = (k << 15) | (k >>> 17);
    k = Math.imul(k, 0x1b873593);

    h ^= k;
    h = (h << 13) | (h >>> 19);
    h = Math.imul(h, 5) + 0xe6546b64;
  }

  // Finalization
  h ^= len;
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  h = Math.imul(h, 0xc2b2ae35);
  h ^= h >>> 16;

  return h >>> 0; // Convert to unsigned 32-bit
}

/**
 * Count leading zeros in a 32-bit integer after skipping the first p bits.
 * Returns the position of the first 1-bit (1-indexed).
 */
function leadingZerosAfterPrecision(hash: number, precision: number): number {
  // Mask off the precision bits
  const remaining = hash << precision;

  // Count leading zeros in remaining bits
  if (remaining === 0) {
    return 32 - precision + 1; // All zeros after precision bits
  }

  return Math.clz32(remaining) + 1;
}

/**
 * Bias correction for HyperLogLog cardinality estimation.
 * These constants are derived empirically for precision p=14.
 */
function alphaMM(m: number): number {
  // For m >= 128, alpha = 0.7213 / (1 + 1.079/m)
  return 0.7213 / (1 + 1.079 / m);
}

/**
 * Small range correction for HyperLogLog.
 * Used when raw estimate is <= 2.5 * m and there are empty registers.
 */
function smallRangeCorrection(m: number, emptyRegisters: number): number {
  return m * Math.log(m / emptyRegisters);
}

/**
 * Large range correction for HyperLogLog.
 * Used when raw estimate is > (1/30) * 2^32.
 */
function largeRangeCorrection(estimate: number): number {
  return -Math.pow(2, 32) * Math.log(1 - estimate / Math.pow(2, 32));
}

export interface HyperLogLogJSON {
  precision: number;
  registers: number[];
}

/**
 * HyperLogLog probabilistic cardinality counter.
 *
 * Supports:
 * - Adding values (streaming updates)
 * - Cardinality estimation with ~1.6% error
 * - Merging multiple sketches
 * - Serialization/deserialization
 *
 * Does NOT support:
 * - Deletion of values (use periodic rebuild instead)
 */
export class HyperLogLog {
  readonly #precision: number;
  readonly #numRegisters: number;
  readonly #registers: Uint8Array;

  constructor(precision = PRECISION) {
    this.#precision = precision;
    this.#numRegisters = 1 << precision;
    this.#registers = new Uint8Array(this.#numRegisters);
  }

  /**
   * Add a value to the sketch.
   * Values are converted to strings before hashing.
   */
  add(value: string | number | boolean | null | undefined): void {
    // Convert value to string for consistent hashing
    const str = String(value);
    const hash = murmurHash3(str);

    // Use first p bits as register index
    const registerIndex = hash & REGISTER_MASK;

    // Count leading zeros in remaining bits
    const leadingZeros = leadingZerosAfterPrecision(hash, this.#precision);

    // Store maximum leading zero count
    this.#registers[registerIndex] = Math.max(
      this.#registers[registerIndex],
      leadingZeros,
    );
  }

  /**
   * Estimate the cardinality (number of distinct values) seen so far.
   * Returns a float estimate with ~1.6% standard error.
   */
  count(): number {
    // Calculate raw estimate using harmonic mean
    let sum = 0;
    let emptyRegisters = 0;

    for (let i = 0; i < this.#numRegisters; i++) {
      const register = this.#registers[i];
      sum += Math.pow(2, -register);
      if (register === 0) {
        emptyRegisters++;
      }
    }

    const rawEstimate =
      alphaMM(this.#numRegisters) *
      this.#numRegisters *
      this.#numRegisters *
      (1 / sum);

    // Apply bias correction based on estimate magnitude
    if (rawEstimate <= 2.5 * this.#numRegisters && emptyRegisters > 0) {
      // Small range correction
      return smallRangeCorrection(this.#numRegisters, emptyRegisters);
    } else if (rawEstimate <= (1 / 30) * Math.pow(2, 32)) {
      // No correction needed
      return rawEstimate;
    } else {
      // Large range correction
      return largeRangeCorrection(rawEstimate);
    }
  }

  /**
   * Merge another HyperLogLog sketch into this one.
   * The merged sketch will contain the union of both sketches.
   *
   * Both sketches must have the same precision.
   */
  merge(other: HyperLogLog): void {
    if (this.#precision !== other.#precision) {
      throw new Error(
        `Cannot merge HyperLogLog sketches with different precision: ${this.#precision} !== ${other.#precision}`,
      );
    }

    // Take maximum of each register
    for (let i = 0; i < this.#numRegisters; i++) {
      this.#registers[i] = Math.max(this.#registers[i], other.#registers[i]);
    }
  }

  /**
   * Serialize the sketch to JSON for persistence.
   */
  toJSON(): HyperLogLogJSON {
    return {
      precision: this.#precision,
      registers: Array.from(this.#registers),
    };
  }

  /**
   * Deserialize a sketch from JSON.
   */
  static fromJSON(json: HyperLogLogJSON): HyperLogLog {
    if (json.precision !== PRECISION) {
      throw new Error(
        `Invalid precision in JSON: expected ${PRECISION}, got ${json.precision}`,
      );
    }

    const hll = new HyperLogLog(json.precision);
    hll.#registers.set(json.registers);
    return hll;
  }

  /**
   * Create a deep copy of this sketch.
   */
  clone(): HyperLogLog {
    return HyperLogLog.fromJSON(this.toJSON());
  }

  /**
   * Reset the sketch to empty state.
   */
  clear(): void {
    this.#registers.fill(0);
  }

  /**
   * Check if the sketch is empty (no values added).
   */
  isEmpty(): boolean {
    return this.#registers.every(r => r === 0);
  }
}
