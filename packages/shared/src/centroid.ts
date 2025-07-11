// Apache License 2.0
// https://github.com/influxdata/tdigest

// Centroid average position of all points in a shape
export class Centroid {
  mean: number;
  weight: number;

  constructor(mean: number, weight: number) {
    this.mean = mean;
    this.weight = weight;
  }

  add(r: Centroid): void {
    if (r.weight < 0) {
      throw new Error('centroid weight cannot be less than zero');
    }
    if (this.weight !== 0) {
      this.weight += r.weight;
      this.mean += (r.weight * (r.mean - this.mean)) / this.weight;
    } else {
      this.weight = r.weight;
      this.mean = r.mean;
    }
  }
}

/** CentroidList is sorted by the mean of the centroid, ascending. */
export type CentroidList = Centroid[];

export function sortCentroidList(centroids: CentroidList): void {
  centroids.sort((a, b) => a.mean - b.mean);
}
