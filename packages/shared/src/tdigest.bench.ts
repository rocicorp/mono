// Apache License 2.0
// https://github.com/influxdata/tdigest

import {generateMersenne53Randomizer} from '@faker-js/faker';
import {bench, describe} from 'vitest';
import {Centroid} from './centroid.js';
import {TDigest} from './tdigest.js';

// Normal distribution using Box-Muller transform
function createNormalDist(mu: number, sigma: number, rand: () => number) {
  return {
    rand: () => {
      let u1;
      do {
        u1 = rand();
      } while (u1 === 0); // Avoids Math.log(0) which is -Infinity
      const u2 = rand();
      const z0 = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
      return z0 * sigma + mu;
    },
  };
}

const N = 1e6;
const mu = 10;
const sigma = 3;

const seed = 42;
const randomizer = generateMersenne53Randomizer(seed);
const rng = () => randomizer.next();

const dist = createNormalDist(mu, sigma, rng);
const uniform = rng;

const uniformData: number[] = [];
const uniformDigest = new TDigest(1000);

const normalData: number[] = [];
const normalDigest = new TDigest(1000);

for (let i = 0; i < N; i++) {
  normalData[i] = dist.rand();
  normalDigest.add(normalData[i], 1);

  uniformData[i] = uniform() * 100;
  uniformDigest.add(uniformData[i], 1);
}

describe('TDigest Benchmarks', () => {
  const quantiles = [0.1, 0.5, 0.9, 0.99, 0.999];

  bench('add', () => {
    const td = new TDigest(1000);
    for (const x of normalData) {
      td.add(x, 1);
    }
  });

  bench('addCentroid', () => {
    const centroids = normalData.map(d => new Centroid(d, 1));

    const td = new TDigest(1000);
    for (const c of centroids) {
      td.addCentroid(c);
    }
  });

  bench('addCentroidList', () => {
    const centroids = normalData.map(d => new Centroid(d, 1));
    const td = new TDigest(1000);
    td.addCentroidList(centroids);
  });

  describe('merge', () => {
    bench('addCentroid', () => {
      const td = new TDigest();
      const cl = normalDigest.centroids();
      for (const c of cl) {
        td.addCentroid(c);
      }
    });
    bench('merge', () => {
      const td = new TDigest();
      td.merge(normalDigest);
    });
  });

  bench('quantile', () => {
    const td = new TDigest(1000);
    for (const x of normalData) {
      td.add(x, 1);
    }
    let x = 0;
    for (const q of quantiles) {
      const val = td.quantile(q);
      if (val !== undefined) {
        x += val;
      }
    }

    use(x);
  });
});

function use(_x: unknown) {}
