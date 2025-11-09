/**
 * HyperLogLog implementation for cardinality estimation.
 *
 * This data structure estimates the number of distinct values in a stream
 * using constant memory, regardless of how many values are processed.
 *
 * Memory: 2^precision bytes (default: 16 KB for p=14)
 * Error: ~1.04 / sqrt(2^precision) (default: ~0.81% for p=14)
 *
 * Based on: "HyperLogLog: the analysis of a near-optimal cardinality
 * estimation algorithm" by Flajolet et al. (2007)
 *
 * @example
 * ```typescript
 * const hll = new HyperLogLog(14); // 16 KB, ~0.8% error
 *
 * // Add values
 * for (const userId of userIds) {
 *   hll.add(userId);
 * }
 *
 * // Get distinct count estimate
 * const distinctUsers = hll.cardinality(); // ~1,234 users
 *
 * // Serialize for storage
 * const json = hll.toJSON();
 * await db.save(JSON.stringify(json));
 *
 * // Deserialize from storage
 * const loaded = HyperLogLog.fromJSON(JSON.parse(jsonStr));
 * ```
 */
export class HyperLogLog {
  private readonly registers: Uint8Array;
  private readonly precision: number;
  private readonly alpha: number;

  /**
   * Create a new HyperLogLog sketch.
   *
   * @param precision - Number of bits for bucket addressing (4-16)
   *   - p=12: 4 KB, ~1.625% error
   *   - p=14: 16 KB, ~0.81% error (recommended)
   *   - p=16: 64 KB, ~0.41% error
   * @param registers - Optional pre-initialized registers (for deserialization)
   */
  constructor(precision: number = 14, registers?: Uint8Array) {
    if (precision < 4 || precision > 16) {
      throw new Error('Precision must be between 4 and 16');
    }

    this.precision = precision;
    const m = 1 << precision; // 2^p buckets

    if (registers) {
      if (registers.length !== m) {
        throw new Error(
          `Register array length (${registers.length}) must equal 2^precision (${m})`,
        );
      }
      this.registers = registers;
    } else {
      this.registers = new Uint8Array(m);
    }

    // Alpha constant for bias correction
    // From the original paper, section 3
    if (m >= 128) {
      this.alpha = 0.7213 / (1 + 1.079 / m);
    } else if (m >= 64) {
      this.alpha = 0.709;
    } else if (m >= 32) {
      this.alpha = 0.697;
    } else {
      this.alpha = 0.5;
    }
  }

  /**
   * Add a value to the sketch.
   *
   * The value will be serialized to a string and hashed. Multiple calls
   * with the same value have no additional effect (set semantics).
   *
   * @param value - The value to add (can be any JSON-serializable type)
   */
  add(value: unknown): void {
    const hash = this.hash64(this.serialize(value));

    // Use first p bits as bucket index
    const bucketIndex = hash & ((1 << this.precision) - 1);

    // Count leading zeros in remaining bits, add 1
    const leadingZeros = this.countLeadingZeros(hash >>> this.precision) + 1;

    // Update register with maximum leading zeros seen for this bucket
    this.registers[bucketIndex] = Math.max(
      this.registers[bucketIndex],
      leadingZeros,
    );
  }

  /**
   * Estimate the number of distinct values added to the sketch.
   *
   * Uses the HyperLogLog algorithm with bias correction for small and
   * large cardinalities.
   *
   * @returns Estimated number of distinct values
   */
  cardinality(): number {
    const m = 1 << this.precision;
    let sum = 0;
    let zeros = 0;

    // Calculate harmonic mean of 2^(-register[i])
    for (let i = 0; i < m; i++) {
      sum += Math.pow(2, -this.registers[i]);
      if (this.registers[i] === 0) zeros++;
    }

    let estimate = (this.alpha * m * m) / sum;

    // Small range correction (from original paper, section 3.4)
    // If estimate <= 2.5m and there are empty registers, use linear counting
    if (estimate <= 2.5 * m) {
      if (zeros !== 0) {
        estimate = m * Math.log(m / zeros);
      }
    }
    // Large range correction (for 32-bit hashes)
    // If estimate > 2^32 / 30, apply correction to avoid overflow bias
    else if (estimate > (1 << 32) / 30) {
      estimate = -(1 << 32) * Math.log(1 - estimate / (1 << 32));
    }

    return Math.round(estimate);
  }

  /**
   * Merge multiple HyperLogLog sketches into a new sketch.
   *
   * This is useful for distributed scenarios where each node maintains
   * its own HLL and you want to combine them for a global estimate.
   *
   * All sketches must have the same precision.
   *
   * @param hlls - Array of HyperLogLog sketches to merge
   * @returns A new HyperLogLog with merged data
   *
   * @example
   * ```typescript
   * const hll1 = new HyperLogLog(14);
   * hll1.add('user1');
   * hll1.add('user2');
   *
   * const hll2 = new HyperLogLog(14);
   * hll2.add('user3');
   * hll2.add('user4');
   *
   * const merged = HyperLogLog.merge([hll1, hll2]);
   * merged.cardinality(); // ~4
   * ```
   */
  static merge(hlls: HyperLogLog[]): HyperLogLog {
    if (hlls.length === 0) {
      throw new Error('Cannot merge empty array');
    }

    const precision = hlls[0].precision;
    if (!hlls.every(h => h.precision === precision)) {
      throw new Error('Cannot merge HLLs with different precision');
    }

    const merged = new HyperLogLog(precision);
    const m = 1 << precision;

    // Take maximum register value across all sketches
    for (let i = 0; i < m; i++) {
      merged.registers[i] = Math.max(...hlls.map(h => h.registers[i]));
    }

    return merged;
  }

  /**
   * Serialize to JSON for storage.
   *
   * The serialized format includes a version number for future compatibility.
   *
   * @returns JSON-serializable object
   */
  toJSON(): HyperLogLogJSON {
    return {
      version: 1,
      precision: this.precision,
      registers: Array.from(this.registers),
    };
  }

  /**
   * Deserialize from JSON.
   *
   * @param json - Previously serialized HyperLogLog
   * @returns Reconstructed HyperLogLog instance
   */
  static fromJSON(json: HyperLogLogJSON): HyperLogLog {
    if (json.version !== 1) {
      throw new Error(`Unsupported HLL version: ${json.version}`);
    }

    const registers = new Uint8Array(json.registers);
    return new HyperLogLog(json.precision, registers);
  }

  /**
   * Get approximate memory usage in bytes.
   *
   * @returns Memory usage in bytes
   */
  memoryUsage(): number {
    return (
      1 <<
      (this.precision + // Register array
        64)
    ); // Object overhead (approximate)
  }

  /**
   * Serialize a value to a string for hashing.
   *
   * Different types get different prefixes to avoid collisions
   * (e.g., number 42 vs string "42").
   *
   * @param value - Value to serialize
   * @returns String representation for hashing
   */
  private serialize(value: unknown): string {
    if (value === null) return '\0null';
    if (value === undefined) return '\0undefined';

    const type = typeof value;

    if (type === 'string') return `s:${value}`;
    if (type === 'number') return `n:${value}`;
    if (type === 'boolean') return `b:${value}`;
    if (type === 'bigint') return `i:${value}`;

    // For objects/arrays, use JSON
    return `j:${JSON.stringify(value)}`;
  }

  /**
   * Hash a string to a 32-bit unsigned integer.
   *
   * Uses a MurmurHash3-inspired algorithm suitable for JavaScript.
   *
   * Note: For production use with very large cardinalities (>1B distinct values),
   * consider using a 64-bit hash or a cryptographic hash to reduce collision probability.
   *
   * @param key - String to hash
   * @returns 32-bit hash value
   */
  private hash64(key: string): number {
    let h = 0;

    for (let i = 0; i < key.length; i++) {
      // Mix character code into hash
      h = Math.imul(h ^ key.charCodeAt(i), 2654435761);
    }

    // Final avalanche
    h = h ^ (h >>> 16);
    h = Math.imul(h, 2246822507);
    h = h ^ (h >>> 13);
    h = Math.imul(h, 3266489909);
    h = h ^ (h >>> 16);

    return h >>> 0; // Convert to unsigned 32-bit
  }

  /**
   * Count leading zeros in a 32-bit integer.
   *
   * @param n - Integer to count leading zeros in
   * @returns Number of leading zeros (0-32)
   */
  private countLeadingZeros(n: number): number {
    if (n === 0) return 32;

    let count = 0;

    // Binary search approach for counting leading zeros
    if ((n & 0xffff0000) === 0) {
      count += 16;
      n <<= 16;
    }
    if ((n & 0xff000000) === 0) {
      count += 8;
      n <<= 8;
    }
    if ((n & 0xf0000000) === 0) {
      count += 4;
      n <<= 4;
    }
    if ((n & 0xc0000000) === 0) {
      count += 2;
      n <<= 2;
    }
    if ((n & 0x80000000) === 0) {
      count += 1;
    }

    return count;
  }
}

/**
 * JSON representation of HyperLogLog for serialization.
 */
export interface HyperLogLogJSON {
  version: 1;
  precision: number;
  registers: number[];
}
