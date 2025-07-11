// Apache License 2.0
// https://github.com/influxdata/tdigest

import {binarySearch} from './binary-search.ts';
import {Centroid, sortCentroidList, type CentroidList} from './centroid.ts';

export interface ReadonlyTDigest {
  readonly count: () => number;
  readonly quantile: (q: number) => number;
  readonly cdf: (x: number) => number;
}

// TDigest is a data structure for accurate on-line accumulation of
// rank-based statistics such as quantiles and trimmed means.
export class TDigest {
  readonly compression: number;

  #maxProcessed: number;
  #maxUnprocessed: number;
  #processed!: CentroidList;
  #unprocessed!: CentroidList;
  #cumulative!: number[];
  #processedWeight!: number;
  #unprocessedWeight!: number;
  #min!: number;
  #max!: number;

  constructor(compression: number = 1000) {
    this.compression = compression;
    this.#maxProcessed = processedSize(0, this.compression);
    this.#maxUnprocessed = unprocessedSize(0, this.compression);
    this.reset();
  }

  reset(): void {
    this.#processed = [];
    this.#unprocessed = [];
    this.#cumulative = [];
    this.#processedWeight = 0;
    this.#unprocessedWeight = 0;
    this.#min = Number.MAX_VALUE;
    this.#max = -Number.MAX_VALUE;
  }

  add(mean: number, weight: number = 1) {
    this.addCentroid(new Centroid(mean, weight));
  }

  /** AddCentroidList can quickly add multiple centroids. */
  addCentroidList(centroidList: CentroidList) {
    for (const c of centroidList) {
      this.addCentroid(c);
    }
  }

  /**
   * AddCentroid adds a single centroid.
   * Weights which are not a number or are <= 0 are ignored, as are NaN means.
   */
  addCentroid(c: Centroid): void {
    if (
      Number.isNaN(c.mean) ||
      c.weight <= 0 ||
      Number.isNaN(c.weight) ||
      !Number.isFinite(c.weight)
    ) {
      return;
    }

    this.#unprocessed.push(new Centroid(c.mean, c.weight));
    this.#unprocessedWeight += c.weight;

    if (
      this.#processed.length > this.#maxProcessed ||
      this.#unprocessed.length > this.#maxUnprocessed
    ) {
      this.#process();
    }
  }

  /**
   *  Merges the supplied digest into this digest. Functionally equivalent to
   * calling t.AddCentroidList(t2.Centroids(nil)), but avoids making an extra
   * copy of the CentroidList.
   **/
  merge(t2: TDigest) {
    t2.#process();
    this.addCentroidList(t2.#processed);
  }

  #process() {
    if (
      this.#unprocessed.length > 0 ||
      this.#processed.length > this.#maxProcessed
    ) {
      // Append all processed centroids to the unprocessed list and sort
      this.#unprocessed.push(...this.#processed);
      sortCentroidList(this.#unprocessed);

      // Reset processed list with first centroid
      this.#processed.length = 0;
      this.#processed.push(this.#unprocessed[0]);

      this.#processedWeight += this.#unprocessedWeight;
      this.#unprocessedWeight = 0;
      let soFar = this.#unprocessed[0].weight;
      let limit = this.#processedWeight * this.#integratedQ(1);
      for (let i = 1; i < this.#unprocessed.length; i++) {
        const centroid = this.#unprocessed[i];
        const projected = soFar + centroid.weight;
        if (projected <= limit) {
          soFar = projected;
          this.#processed[this.#processed.length - 1].add(centroid);
        } else {
          const k1 = this.#integratedLocation(soFar / this.#processedWeight);
          limit = this.#processedWeight * this.#integratedQ(k1 + 1);
          soFar += centroid.weight;
          this.#processed.push(centroid);
        }
      }
      this.#min = Math.min(this.#min, this.#processed[0].mean);
      this.#max = Math.max(
        this.#max,
        this.#processed[this.#processed.length - 1].mean,
      );
      this.#unprocessed.length = 0;
    }
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
    return cl.concat(this.#processed);
  }

  count(): number {
    this.#process();

    // this.process always updates this.processedWeight to the total count of all
    // centroids, so we don't need to re-count here.
    return this.#processedWeight;
  }

  #updateCumulative() {
    // Weight can only increase, so the final cumulative value will always be
    // either equal to, or less than, the total weight. If they are the same,
    // then nothing has changed since the last update.
    if (
      this.#cumulative.length > 0 &&
      this.#cumulative[this.#cumulative.length - 1] === this.#processedWeight
    ) {
      return;
    }
    const n = this.#processed.length + 1;
    if (this.#cumulative.length > n) {
      this.#cumulative.length = n;
    }

    let prev = 0;
    for (let i = 0; i < this.#processed.length; i++) {
      const centroid = this.#processed[i];
      const cur = centroid.weight;
      this.#cumulative[i] = prev + cur / 2;
      prev += cur;
    }
    this.#cumulative[this.#processed.length] = prev;
  }

  // Quantile returns the (approximate) quantile of
  // the distribution. Accepted values for q are between 0 and 1.
  // Returns NaN if Count is zero or bad inputs.
  quantile(q: number): number {
    this.#process();
    this.#updateCumulative();
    if (q < 0 || q > 1 || this.#processed.length === 0) {
      return NaN;
    }
    if (this.#processed.length === 1) {
      return this.#processed[0].mean;
    }
    const index = q * this.#processedWeight;
    if (index <= this.#processed[0].weight / 2) {
      return (
        this.#min +
        ((2 * index) / this.#processed[0].weight) *
          (this.#processed[0].mean - this.#min)
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
        this.#processed[lower - 1].mean,
        z2,
        this.#processed[lower].mean,
        z1,
      );
    }

    const z1 =
      index - this.#processedWeight - this.#processed[lower - 1].weight / 2;
    const z2 = this.#processed[lower - 1].weight / 2 - z1;
    return weightedAverage(
      this.#processed[this.#processed.length - 1].mean,
      z1,
      this.#max,
      z2,
    );
  }

  /**
   * CDF returns the cumulative distribution function for a given value x.
   */
  cdf(x: number): number {
    this.#process();
    this.#updateCumulative();
    switch (this.#processed.length) {
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
    const m0 = this.#processed[0].mean;
    // Left Tail
    if (x <= m0) {
      if (m0 - this.#min > 0) {
        return (
          (((x - this.#min) / (m0 - this.#min)) * this.#processed[0].weight) /
          this.#processedWeight /
          2
        );
      }
      return 0;
    }
    // Right Tail
    const mn = this.#processed[this.#processed.length - 1].mean;
    if (x >= mn) {
      if (this.#max - mn > 0) {
        return (
          1 -
          (((this.#max - x) / (this.#max - mn)) *
            this.#processed[this.#processed.length - 1].weight) /
            this.#processedWeight /
            2
        );
      }
      return 1;
    }

    const upper = binarySearch(
      this.#processed.length,
      // Treat equals as greater than, so we can use the upper index
      // This is equivalent to:
      //   i => this.#processed[i].mean > x ? -1 : 1,
      i => x - this.#processed[i].mean || 1,
    );

    const z1 = x - this.#processed[upper - 1].mean;
    const z2 = this.#processed[upper].mean - x;
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

function processedSize(size: number, compression: number): number {
  if (size === 0) {
    return Math.ceil(compression) * 2;
  }
  return size;
}

function unprocessedSize(size: number, compression: number): number {
  if (size === 0) {
    return Math.ceil(compression) * 8;
  }
  return size;
}
