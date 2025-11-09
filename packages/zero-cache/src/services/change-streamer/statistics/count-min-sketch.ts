/**
 * Count-Min Sketch for frequency estimation.
 *
 * This data structure estimates how many times specific values appear in a stream
 * using sub-linear memory. It can over-estimate but never under-estimates frequencies.
 *
 * Memory: width × depth × 4 bytes (default: ~20 KB for ε=0.01, δ=0.001)
 * Error: Returns true frequency + ε × total_count with probability 1-δ
 *
 * Based on: "An improved data stream summary: the count-min sketch and its
 * applications" by Cormode and Muthukrishnan (2005)
 *
 * @example
 * ```typescript
 * const cms = new CountMinSketch(0.01, 0.001); // 1% error, 99.9% confidence
 *
 * // Add values
 * cms.add('user_id_5');
 * cms.add('user_id_5');
 * cms.add('user_id_10');
 *
 * // Query frequency
 * cms.query('user_id_5'); // ~2
 * cms.query('user_id_10'); // ~1
 *
 * // Support deletion (negative counts)
 * cms.add('user_id_5', -1); // Remove one occurrence
 * cms.query('user_id_5'); // ~1
 * ```
 */
export class CountMinSketch {
  private readonly counters: Uint32Array[];
  private readonly width: number;
  private readonly depth: number;
  private readonly seeds: number[];

  /**
   * Create a new Count-Min Sketch.
   *
   * @param epsilon - Error bound (e.g., 0.01 for 1% error)
   * @param delta - Failure probability (e.g., 0.001 for 99.9% confidence)
   * @param counters - Optional pre-initialized counters (for deserialization)
   * @param seeds - Optional hash seeds (for deserialization)
   */
  constructor(
    epsilon: number = 0.01,
    delta: number = 0.001,
    counters?: Uint32Array[],
    seeds?: number[],
  ) {
    if (epsilon <= 0 || epsilon >= 1) {
      throw new Error('Epsilon must be between 0 and 1');
    }
    if (delta <= 0 || delta >= 1) {
      throw new Error('Delta must be between 0 and 1');
    }

    // Calculate dimensions from error parameters
    // Width: w = ceil(e / ε)
    // Depth: d = ceil(ln(1/δ))
    this.width = Math.ceil(Math.E / epsilon);
    this.depth = Math.ceil(Math.log(1 / delta));

    if (counters && seeds) {
      // Deserializing - validate dimensions
      if (counters.length !== this.depth) {
        throw new Error(
          `Counter depth (${counters.length}) must equal ${this.depth}`,
        );
      }
      if (!counters.every(row => row.length === this.width)) {
        throw new Error(`All counter rows must have width ${this.width}`);
      }
      if (seeds.length !== this.depth) {
        throw new Error(
          `Seeds length (${seeds.length}) must equal ${this.depth}`,
        );
      }

      this.counters = counters;
      this.seeds = seeds;
    } else {
      // Fresh initialization
      this.counters = Array.from(
        {length: this.depth},
        () => new Uint32Array(this.width),
      );

      // Generate random seeds for hash functions
      this.seeds = Array.from(
        {length: this.depth},
        (_, i) => Math.floor(Math.random() * 0x7fffffff) + i,
      );
    }
  }

  /**
   * Add a value to the sketch (increment its count).
   *
   * Supports negative counts for deletion, but be aware that this can lead
   * to underestimation if not used carefully.
   *
   * @param value - The value to add
   * @param count - How much to add (default: 1, can be negative)
   */
  add(value: unknown, count: number = 1): void {
    const key = this.serialize(value);

    for (let i = 0; i < this.depth; i++) {
      const hash = this.hash(key, this.seeds[i]);
      const index = hash % this.width;
      this.counters[i][index] += count;
    }
  }

  /**
   * Query the estimated frequency of a value.
   *
   * Returns the minimum count across all hash functions. The true frequency
   * is guaranteed to be ≤ the returned value (may over-estimate).
   *
   * @param value - The value to query
   * @returns Estimated frequency (≥ true frequency)
   */
  query(value: unknown): number {
    const key = this.serialize(value);
    let min = Infinity;

    for (let i = 0; i < this.depth; i++) {
      const hash = this.hash(key, this.seeds[i]);
      const index = hash % this.width;
      min = Math.min(min, this.counters[i][index]);
    }

    return min === Infinity ? 0 : min;
  }

  /**
   * Merge multiple Count-Min Sketches into a new sketch.
   *
   * All sketches must have the same dimensions (width and depth).
   * The merge operation sums counters element-wise.
   *
   * @param sketches - Array of Count-Min Sketches to merge
   * @returns A new Count-Min Sketch with merged data
   *
   * @example
   * ```typescript
   * const cms1 = new CountMinSketch(0.01, 0.001);
   * cms1.add('user_5', 10);
   *
   * const cms2 = new CountMinSketch(0.01, 0.001);
   * cms2.add('user_5', 20);
   *
   * const merged = CountMinSketch.merge([cms1, cms2]);
   * merged.query('user_5'); // ~30
   * ```
   */
  static merge(sketches: CountMinSketch[]): CountMinSketch {
    if (sketches.length === 0) {
      throw new Error('Cannot merge empty array');
    }

    const first = sketches[0];
    if (
      !sketches.every(s => s.width === first.width && s.depth === first.depth)
    ) {
      throw new Error('Cannot merge CMS with different dimensions');
    }

    // Create new sketch with same dimensions
    const merged = Object.create(CountMinSketch.prototype);
    merged.width = first.width;
    merged.depth = first.depth;
    merged.seeds = [...first.seeds];
    merged.counters = Array.from(
      {length: first.depth},
      () => new Uint32Array(first.width),
    );

    // Sum all counters element-wise
    for (let i = 0; i < first.depth; i++) {
      for (let j = 0; j < first.width; j++) {
        merged.counters[i][j] = sketches.reduce(
          (sum, s) => sum + s.counters[i][j],
          0,
        );
      }
    }

    return merged;
  }

  /**
   * Serialize to JSON for storage.
   *
   * @returns JSON-serializable object
   */
  toJSON(): CountMinSketchJSON {
    return {
      version: 1,
      width: this.width,
      depth: this.depth,
      seeds: this.seeds,
      counters: this.counters.map(row => Array.from(row)),
    };
  }

  /**
   * Deserialize from JSON.
   *
   * @param json - Previously serialized Count-Min Sketch
   * @returns Reconstructed Count-Min Sketch instance
   */
  static fromJSON(json: CountMinSketchJSON): CountMinSketch {
    if (json.version !== 1) {
      throw new Error(`Unsupported CMS version: ${json.version}`);
    }

    const counters = json.counters.map(row => new Uint32Array(row));
    const epsilon = Math.E / json.width;
    const delta = Math.exp(-json.depth);

    return new CountMinSketch(epsilon, delta, counters, json.seeds);
  }

  /**
   * Get approximate memory usage in bytes.
   *
   * @returns Memory usage in bytes
   */
  memoryUsage(): number {
    return (
      this.width * this.depth * 4 + // Counters (4 bytes each)
      this.depth * 8 + // Seeds (8 bytes each, approximate)
      128
    ); // Object overhead
  }

  /**
   * Serialize a value to a string for hashing.
   *
   * Different types get different prefixes to avoid collisions.
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
   * Hash a string with a seed to a 32-bit unsigned integer.
   *
   * Uses a simple but effective hash function suitable for JavaScript.
   *
   * @param key - String to hash
   * @param seed - Hash seed for this hash function
   * @returns 32-bit hash value
   */
  private hash(key: string, seed: number): number {
    let h = seed;

    for (let i = 0; i < key.length; i++) {
      h = Math.imul(h ^ key.charCodeAt(i), 2654435761);
    }

    // Avalanche
    h = h ^ (h >>> 16);
    h = Math.imul(h, 2246822507);
    h = h ^ (h >>> 13);

    return (h >>> 0) % this.width;
  }
}

/**
 * JSON representation of Count-Min Sketch for serialization.
 */
export interface CountMinSketchJSON {
  version: 1;
  width: number;
  depth: number;
  seeds: number[];
  counters: number[][];
}
