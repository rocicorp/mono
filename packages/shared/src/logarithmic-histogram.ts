/**
 * Histogram class for tracking the distribution of values in logarithmic buckets.
 *
 * This histogram divides a range of positive values into logarithmically-spaced buckets
 * and counts how many values fall into each bucket. It's particularly useful for data
 * that spans several orders of magnitude.
 *
 * Features:
 * - Logarithmic bucketing for better representation of exponentially distributed data
 * - Underflow and overflow buckets for values outside the specified range
 * - Serialization to/from hex strings for persistence
 * - Efficient storage using Uint32Array
 *
 * Example usage:
 * ```js
 * const hist = new Histogram(); // min=1
 * hist.add(5);
 * hist.add(250);
 * console.log(hist.counts);
 * ```
 */
export class LogarithmicHistogram {
  /** Array holding counts for all buckets (including underflow at index 0) */
  #counts = new Uint32Array(2); // Start with underflow and one bucket

  /**
   * Adds a value to the appropriate bucket in the histogram.
   *
   * @param value - The positive number to add to the histogram
   * @throws Error if value is less than or equal to zero
   */
  add(value: number) {
    if (value < 0) {
      throw new Error(`Value must not be negative, got: ${value}`);
    }
    if (value < 1) {
      this.#counts[0]++;
      return;
    }

    const index = Math.floor(Math.log2(value)) + 1;

    // Resize if index exceeds current array size
    if (index >= this.#counts.length) {
      const newCounts = new Uint32Array(index + 1);
      newCounts.set(this.#counts);
      this.#counts = newCounts;
    }

    this.#counts[index]++;
  }

  /**
   * Returns a read-only view of the bucket counts.
   *
   * The first element (index 0) is the underflow bucket.
   * All other are regular logarithmic buckets.
   *
   * @returns A read-only array of bucket counts
   */
  get counts(): Readonly<Uint32Array> {
    return this.#counts;
  }

  /**
   * Calculates and returns the value ranges for each bucket.
   *
   * @returns An array of tuples, each containing [min, max] for a bucket.
   *          The first tuple represents the underflow bucket.
   *          For regular buckets, the range is [min, max), where min is inclusive and max is exclusive.
   */
  getBucketRanges(): [number, number][] {
    const ranges: [number, number][] = [[0, 1]];
    for (let i = 1; i < this.#counts.length; i++) {
      ranges.push([2 ** (i - 1), 2 ** i]);
    }
    return ranges;
  }

  /**
   * Serializes the histogram data to a hex string.
   *
   * Each count is encoded as 8 hexadecimal characters (4 bytes).
   *
   * @returns A hex string representing the histogram data
   */
  toHexString(): string {
    return Array.from(this.#counts, count =>
      count.toString(16).padStart(8, '0'),
    ).join('');
  }

  /**
   * Creates a histogram from a serialized hex string.
   *
   * @param hex - The hex string representation of histogram counts
   * @returns A new Histogram instance with the deserialized data
   * @throws Error if the hex string format is invalid
   */
  static fromHexString(hex: string): LogarithmicHistogram {
    const histogram = new LogarithmicHistogram();
    const counts = new Uint32Array(hex.length / 8);
    for (let i = 0; i < hex.length; i += 8) {
      const n = parseInt(hex.slice(i, i + 8), 16);
      if (isNaN(n)) {
        throw new Error(`Invalid hex string: ${hex}`);
      }
      counts[i / 8] = n;
    }
    histogram.#counts = counts;
    return histogram;
  }
}
