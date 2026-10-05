// Apache License 2.0
// https://github.com/influxdata/tdigest

import {binarySearch} from './binary-search.ts';
import {Centroid, type CentroidList} from './centroid.ts';
import type {TDigestJSON} from './tdigest-schema.ts';

export interface ReadonlyTDigest {
  readonly count: () => number;
  readonly quantile: (q: number) => number;
  readonly cdf: (x: number) => number;
}

/** Initial capacity of the buffer of unprocessed values. */
const INITIAL_CAPACITY = 16;

// TDigest is a data structure for accurate on-line accumulation of
// rank-based statistics such as quantiles and trimmed means.
//
// Unlike the Go original, centroids are kept as parallel arrays of means and
// weights rather than as objects, so adding a value allocates nothing. New
// values are buffered in a Float64Array and sorted with its native numeric
// sort instead of Array.prototype.sort with a comparator. On an engine
// without a JIT, such as Hermes in React Native, calling the comparator for
// every comparison is what made adding values expensive.
export class TDigest {
  readonly compression: number;

  readonly #maxUnprocessed: number;

  /** Means of the processed centroids, ascending. */
  #means!: number[];
  /** Weights of the processed centroids, parallel to `#means`. */
  #weights!: number[];

  /** Means of the values added since the last `#process`, in order. */
  #unprocessedMeans: Float64Array;
  /** Weights of the unprocessed values, parallel to `#unprocessedMeans`. */
  #unprocessedWeights: Float64Array;
  #unprocessedCount!: number;

  #cumulative!: number[];
  #processedWeight!: number;
  #unprocessedWeight!: number;
  #min!: number;
  #max!: number;

  /**
   * A digest keeps roughly `compression` centroids, and buffers up to eight
   * times as many values between merges. The Go original defaults to 1000;
   * 100 keeps p50, p95 and p99 within about 0.5% for a tenth of the memory
   * and serialized size, which is plenty for the inspector's percentiles.
   */
  constructor(compression: number = 100) {
    this.compression = compression;
    this.#maxUnprocessed = unprocessedSize(0, this.compression);
    const capacity = Math.min(INITIAL_CAPACITY, this.#maxUnprocessed + 1);
    this.#unprocessedMeans = new Float64Array(capacity);
    this.#unprocessedWeights = new Float64Array(capacity);
    this.reset();
  }

  /**
   * fromJSON creates a TDigest from a JSON-serializable representation.
   * The data should be an object with compression and centroids array.
   */
  static fromJSON(data: Readonly<TDigestJSON>): TDigest {
    const digest = new TDigest(data[0]);
    if (data.length % 2 !== 1) {
      throw new Error('Invalid centroids array');
    }
    for (let i = 1; i < data.length; i += 2) {
      digest.add(data[i], data[i + 1]);
    }
    return digest;
  }

  reset(): void {
    this.#means = [];
    this.#weights = [];
    this.#unprocessedCount = 0;
    this.#cumulative = [];
    this.#processedWeight = 0;
    this.#unprocessedWeight = 0;
    this.#min = Number.MAX_VALUE;
    this.#max = -Number.MAX_VALUE;
  }

  /**
   * Adds a value with the given weight.
   * Weights which are not a number or are <= 0 are ignored, as are NaN means.
   */
  add(mean: number, weight: number = 1): void {
    if (Number.isNaN(mean) || weight <= 0 || !Number.isFinite(weight)) {
      return;
    }

    const n = this.#unprocessedCount;
    if (n === this.#unprocessedMeans.length) {
      this.#grow();
    }
    this.#unprocessedMeans[n] = mean;
    this.#unprocessedWeights[n] = weight;
    this.#unprocessedCount = n + 1;
    this.#unprocessedWeight += weight;

    if (this.#unprocessedCount > this.#maxUnprocessed) {
      this.#process();
    }
  }

  /** AddCentroidList can quickly add multiple centroids. */
  addCentroidList(centroidList: CentroidList) {
    for (const c of centroidList) {
      this.add(c.mean, c.weight);
    }
  }

  /**
   * AddCentroid adds a single centroid.
   * Weights which are not a number or are <= 0 are ignored, as are NaN means.
   */
  addCentroid(c: Centroid): void {
    this.add(c.mean, c.weight);
  }

  /**
   *  Merges the supplied digest into this digest. Functionally equivalent to
   * calling t.AddCentroidList(t2.Centroids(nil)), but avoids making an extra
   * copy of the CentroidList.
   **/
  merge(t2: TDigest) {
    t2.#process();
    // #process replaces the arrays rather than changing them, so these stay
    // intact even if t2 is this digest.
    const means = t2.#means;
    const weights = t2.#weights;
    for (let i = 0; i < means.length; i++) {
      this.add(means[i], weights[i]);
    }
  }

  /** Doubles the unprocessed buffer, up to what `add` lets it hold. */
  #grow(): void {
    const capacity = Math.min(
      this.#unprocessedMeans.length * 2,
      this.#maxUnprocessed + 1,
    );
    const means = new Float64Array(capacity);
    means.set(this.#unprocessedMeans);
    this.#unprocessedMeans = means;
    const weights = new Float64Array(capacity);
    weights.set(this.#unprocessedWeights);
    this.#unprocessedWeights = weights;
  }

  /**
   * Merges the unprocessed values into the centroids: walks both in order of
   * mean, folding each into the last centroid while that stays within its
   * size limit, and starting a new centroid otherwise.
   */
  #process() {
    if (this.#unprocessedCount === 0) {
      return;
    }
    const [newMeans, newWeights] = this.#sortedUnprocessed();
    const oldMeans = this.#means;
    const oldWeights = this.#weights;
    const n = newMeans.length;
    const p = oldMeans.length;
    this.#unprocessedCount = 0;
    this.#processedWeight += this.#unprocessedWeight;
    this.#unprocessedWeight = 0;
    const total = this.#processedWeight;

    const means: number[] = [];
    const weights: number[] = [];
    // The centroid being built, kept in locals rather than in the arrays,
    // which is markedly faster without a JIT. Its weight is 0 until the first
    // value.
    let mean = 0;
    let weight = 0;
    let soFar = 0;
    // A limit of 0 makes the first value start a centroid, whose limit then
    // comes from the same formula as every other's. With soFar at 0 that is
    // integratedQ(1): integratedLocation(0) is exactly 0, since
    // Math.asin(-1) is -Math.PI / 2.
    let limit = 0;
    let i = 0;
    let j = 0;
    while (i < n || j < p) {
      // On a tie the new value goes first, as in the original, which appended
      // the old centroids to the new values and sorted them with a stable sort.
      let m: number;
      let w: number;
      if (i < n && (j === p || newMeans[i] <= oldMeans[j])) {
        m = newMeans[i];
        w = newWeights[i++];
      } else {
        m = oldMeans[j];
        w = oldWeights[j++];
      }
      if (soFar + w <= limit) {
        // Same arithmetic as Centroid.add.
        weight += w;
        mean += (w * (m - mean)) / weight;
      } else {
        if (weight > 0) {
          means.push(mean);
          weights.push(weight);
        }
        const k = this.#integratedLocation(soFar / total);
        limit = total * this.#integratedQ(k + 1);
        mean = m;
        weight = w;
      }
      soFar += w;
    }
    means.push(mean);
    weights.push(weight);

    this.#means = means;
    this.#weights = weights;
    this.#min = Math.min(this.#min, means[0]);
    // oxlint-disable-next-line typescript/no-non-null-assertion
    this.#max = Math.max(this.#max, means.at(-1)!);
  }

  /** The unprocessed means and weights, sorted by mean. */
  #sortedUnprocessed(): [Float64Array, Float64Array] {
    const n = this.#unprocessedCount;
    const means = this.#unprocessedMeans.subarray(0, n);
    const weights = this.#unprocessedWeights.subarray(0, n);
    // fromJSON and merge add centroids in order of mean.
    if (isSorted(means)) {
      return [means, weights];
    }
    // With one weight for all the values, sorting the means alone keeps every
    // value with its weight, and the native numeric sort needs no comparator.
    if (allEqual(weights)) {
      means.sort();
      return [means, weights];
    }
    // A stable sort, so values with equal means stay in the order added.
    const order = Array.from(means, (_, k) => k).sort(
      (a, b) => means[a] - means[b],
    );
    return [
      Float64Array.from(order, k => means[k]),
      Float64Array.from(order, k => weights[k]),
    ];
  }

  /**
   * Centroids returns a copy of processed centroids.
   * Useful when aggregating multiple t-digests.
   *
   * Centroids are appended to the passed CentroidList; if you're re-using a
   * buffer, be sure to pass cl[:0].
   */
  centroids(cl: CentroidList = []): CentroidList {
    this.#process();
    const result = [...cl];
    for (let i = 0; i < this.#means.length; i++) {
      result.push(new Centroid(this.#means[i], this.#weights[i]));
    }
    return result;
  }

  count(): number {
    this.#process();

    // this.process always updates this.processedWeight to the total count of all
    // centroids, so we don't need to re-count here.
    return this.#processedWeight;
  }

  /**
   * toJSON returns a JSON-serializable representation of the digest.
   * This processes the digest and returns an object with compression and centroid data.
   */
  toJSON(): TDigestJSON {
    this.#process();
    const data: TDigestJSON = [this.compression];
    for (let i = 0; i < this.#means.length; i++) {
      data.push(this.#means[i], this.#weights[i]);
    }
    return data;
  }

  #updateCumulative() {
    // Weight can only increase, so the final cumulative value will always be
    // either equal to, or less than, the total weight. If they are the same,
    // then nothing has changed since the last update.
    if (
      this.#cumulative.length > 0 &&
      this.#cumulative.at(-1) === this.#processedWeight
    ) {
      return;
    }
    const n = this.#means.length + 1;
    if (this.#cumulative.length > n) {
      this.#cumulative.length = n;
    }

    let prev = 0;
    for (let i = 0; i < this.#means.length; i++) {
      const cur = this.#weights[i];
      this.#cumulative[i] = prev + cur / 2;
      prev += cur;
    }
    this.#cumulative[this.#means.length] = prev;
  }

  // Quantile returns the (approximate) quantile of
  // the distribution. Accepted values for q are between 0 and 1.
  // Returns NaN if Count is zero or bad inputs.
  quantile(q: number): number {
    this.#process();
    this.#updateCumulative();
    if (q < 0 || q > 1 || this.#means.length === 0) {
      return NaN;
    }
    if (this.#means.length === 1) {
      return this.#means[0];
    }
    const index = q * this.#processedWeight;
    if (index <= this.#weights[0] / 2) {
      return (
        this.#min +
        ((2 * index) / this.#weights[0]) * (this.#means[0] - this.#min)
      );
    }

    const lower = binarySearch(
      this.#cumulative.length,
      (i: number) => -this.#cumulative[i] + index,
    );

    if (lower + 1 !== this.#cumulative.length) {
      const z1 = index - this.#cumulative[lower - 1];
      const z2 = this.#cumulative[lower] - index;
      return weightedAverage(
        this.#means[lower - 1],
        z2,
        this.#means[lower],
        z1,
      );
    }

    const z1 = index - this.#processedWeight - this.#weights[lower - 1] / 2;
    const z2 = this.#weights[lower - 1] / 2 - z1;
    // oxlint-disable-next-line typescript/no-non-null-assertion
    return weightedAverage(this.#means.at(-1)!, z1, this.#max, z2);
  }

  /**
   * CDF returns the cumulative distribution function for a given value x.
   */
  cdf(x: number): number {
    this.#process();
    this.#updateCumulative();
    switch (this.#means.length) {
      case 0:
        return 0;
      case 1: {
        const width = this.#max - this.#min;
        if (x <= this.#min) {
          return 0;
        }
        if (x >= this.#max) {
          return 1;
        }
        if (x - this.#min <= width) {
          // min and max are too close together to do any viable interpolation
          return 0.5;
        }
        return (x - this.#min) / width;
      }
    }

    if (x <= this.#min) {
      return 0;
    }
    if (x >= this.#max) {
      return 1;
    }
    const m0 = this.#means[0];
    // Left Tail
    if (x <= m0) {
      if (m0 - this.#min > 0) {
        return (
          (((x - this.#min) / (m0 - this.#min)) * this.#weights[0]) /
          this.#processedWeight /
          2
        );
      }
      return 0;
    }
    // Right Tail
    // oxlint-disable-next-line typescript/no-non-null-assertion
    const mn = this.#means.at(-1)!;
    if (x >= mn) {
      if (this.#max - mn > 0) {
        return (
          1 -
          (((this.#max - x) / (this.#max - mn)) *
            // oxlint-disable-next-line typescript/no-non-null-assertion
            this.#weights.at(-1)!) /
            this.#processedWeight /
            2
        );
      }
      return 1;
    }

    const upper = binarySearch(
      this.#means.length,
      // Treat equals as greater than, so we can use the upper index
      // This is equivalent to:
      //   i => this.#means[i] > x ? -1 : 1,
      i => x - this.#means[i] || 1,
    );

    const z1 = x - this.#means[upper - 1];
    const z2 = this.#means[upper] - x;
    return (
      weightedAverage(
        this.#cumulative[upper - 1],
        z2,
        this.#cumulative[upper],
        z1,
      ) / this.#processedWeight
    );
  }

  #integratedQ(k: number): number {
    return (
      (Math.sin(
        (Math.min(k, this.compression) * Math.PI) / this.compression -
          Math.PI / 2,
      ) +
        1) /
      2
    );
  }

  #integratedLocation(q: number): number {
    return (this.compression * (Math.asin(2 * q - 1) + Math.PI / 2)) / Math.PI;
  }
}

// Calculate number of bytes needed for a tdigest of size c,
// where c is the compression value
export function byteSizeForCompression(comp: number): number {
  const c = comp | 0;
  // // A centroid is 2 float64s, so we need 16 bytes for each centroid
  // float_size := 8
  // centroid_size := 2 * float_size

  // // Unprocessed and processed can grow up to length c
  // unprocessed_size := centroid_size * c
  // processed_size := unprocessed_size

  // // the cumulative field can also be of length c, but each item is a single float64
  // cumulative_size := float_size * c // <- this could also be unprocessed_size / 2

  // return unprocessed_size + processed_size + cumulative_size

  // // or, more succinctly:
  // return float_size * c * 5

  // or even more succinctly
  return c * 40;
}

function isSorted(values: Float64Array): boolean {
  for (let i = 1; i < values.length; i++) {
    if (values[i] < values[i - 1]) {
      return false;
    }
  }
  return true;
}

function allEqual(values: Float64Array): boolean {
  for (let i = 1; i < values.length; i++) {
    if (values[i] !== values[0]) {
      return false;
    }
  }
  return true;
}

function weightedAverage(
  x1: number,
  w1: number,
  x2: number,
  w2: number,
): number {
  if (x1 <= x2) {
    return weightedAverageSorted(x1, w1, x2, w2);
  }
  return weightedAverageSorted(x2, w2, x1, w1);
}

function weightedAverageSorted(
  x1: number,
  w1: number,
  x2: number,
  w2: number,
): number {
  const x = (x1 * w1 + x2 * w2) / (w1 + w2);
  return Math.max(x1, Math.min(x, x2));
}

function unprocessedSize(size: number, compression: number): number {
  if (size === 0) {
    return Math.ceil(compression) * 8;
  }
  return size;
}
