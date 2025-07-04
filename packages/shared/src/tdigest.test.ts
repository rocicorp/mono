// Apache License 2.0
// https://github.com/influxdata/tdigest

import {generateMersenne53Randomizer} from '@faker-js/faker';
import {describe, expect, test} from 'vitest';
import {Centroid} from './centroid.ts';
import {TDigest} from './tdigest.ts';

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

// Compares the quantile results of two digests, and fails if the
// fractional err exceeds maxErr.
// Always fails if the total count differs.
function compareQuantiles(td1: TDigest, td2: TDigest, maxErr: number) {
  expect(td1.count()).toBe(td2.count());

  for (let q = 0.05; q < 1; q += 0.05) {
    if (
      Math.abs(td1.quantile(q) - td2.quantile(q)) / td1.quantile(q) >
      maxErr
    ) {
      throw new Error(
        `quantile ${q} differs, ${td1.quantile(q)} vs ${td2.quantile(q)}`,
      );
    }
  }
}

describe('TDigest', () => {
  // All Add methods should yield equivalent results.
  test('Add functions', () => {
    const centroids = normalDigest.centroids();

    const addDigest = new TDigest(100);
    const addCentroidDigest = new TDigest(100);
    const addCentroidListDigest = new TDigest(100);

    for (const c of centroids) {
      addDigest.add(c.mean, c.weight);
      addCentroidDigest.addCentroid(c);
    }
    addCentroidListDigest.addCentroidList(centroids);

    expect(() =>
      compareQuantiles(addDigest, addCentroidDigest, 0.01),
    ).not.toThrow();
    expect(() =>
      compareQuantiles(addDigest, addCentroidListDigest, 0.01),
    ).not.toThrow();
  });

  describe('count', () => {
    const tests = [
      {
        name: 'empty',
        data: [],
        want: 0,
      },
      {
        name: 'not empty',
        data: [5, 4],
        want: 2,
      },
    ];

    test.each(tests)('$name', ({data, want}) => {
      const td = new TDigest(1000);
      for (const x of data) {
        td.add(x, 1);
      }
      const got = td.count();
      expect(got).toBe(want);
    });

    test('normalDigest and uniformDigest', () => {
      let got = normalDigest.count();
      let want = normalData.length;
      expect(got).toBe(want);

      got = uniformDigest.count();
      want = uniformData.length;
      expect(got).toBe(want);
    });
  });

  describe('quantile', () => {
    const tests = () => [
      {
        name: 'increasing',
        quantile: 0.5,
        data: [1, 2, 3, 4, 5],
        want: 3,
      },
      {
        name: 'data in decreasing order',
        quantile: 0.25,
        data: [555.349107, 432.842597],
        want: 432.842597,
      },
      {
        name: 'small',
        quantile: 0.5,
        data: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1],
        want: 3,
      },
      {
        name: 'small 99 (max)',
        quantile: 0.99,
        data: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1],
        want: 5,
      },
      {
        name: 'normal 50',
        quantile: 0.5,
        digest: normalDigest,
        want: 10.00023294114162,
      },
      {
        name: 'normal 90',
        quantile: 0.9,
        digest: normalDigest,
        want: 13.846969895458521,
      },
      {
        name: 'uniform 50',
        quantile: 0.5,
        digest: uniformDigest,
        want: 49.98262428008381,
      },
      {
        name: 'uniform 90',
        quantile: 0.9,
        digest: uniformDigest,
        want: 89.9852939369368,
      },
      {
        name: 'uniform 99',
        quantile: 0.99,
        digest: uniformDigest,
        want: 99.00143885611,
      },
      {
        name: 'uniform 99.9',
        quantile: 0.999,
        digest: uniformDigest,
        want: 99.8969094946892,
      },
    ];
    test.each(tests())('$name', ({digest, data, quantile, want}) => {
      let td = digest;
      if (td === undefined) {
        td = new TDigest(1000);
        for (const x of data!) {
          td.add(x, 1);
        }
      }
      const got = td.quantile(quantile);
      expect(got).toBe(want);
    });
  });

  describe('cdf', () => {
    const tests = [
      {
        name: 'increasing',
        cdf: 3,
        data: [1, 2, 3, 4, 5],
        want: 0.5,
      },
      {
        name: 'small',
        cdf: 4,
        data: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1],
        want: 0.75,
      },
      {
        name: 'small max',
        cdf: 5,
        data: [1, 2, 3, 4, 5, 5, 4, 3, 2, 1],
        want: 1,
      },
      {
        name: 'normal mean',
        cdf: 10,
        data: normalData,
        want: 0.5,
      },
      {
        name: 'normal high',
        cdf: -100,
        data: normalData,
        want: 0,
      },
      {
        name: 'normal low',
        cdf: 110,
        data: normalData,
        want: 1,
      },
      {
        name: 'uniform 50',
        cdf: 50,
        data: uniformData,
        want: 0.5,
      },
      {
        name: 'uniform min',
        cdf: 0,
        data: uniformData,
        want: 0,
      },
      {
        name: 'uniform max',
        cdf: 100,
        data: uniformData,
        want: 1,
      },
      {
        name: 'uniform 10',
        cdf: 10,
        data: uniformData,
        want: 0.1,
      },
      {
        name: 'uniform 90',
        cdf: 90,
        data: uniformData,
        want: 0.9,
      },
    ];
    test.each(tests)('$name', ({data, cdf, want}) => {
      const td = new TDigest(1000);
      for (const x of data) {
        td.add(x, 1);
      }
      const got = td.cdf(cdf);
      expect(got).toBeCloseTo(want);
    });
  });

  test('reset', () => {
    const td = new TDigest();
    for (const x of normalData) {
      td.add(x, 1);
    }
    const q1 = td.quantile(0.9);

    td.reset();
    for (const x of normalData) {
      td.add(x, 1);
    }
    expect(q1).toBeDefined;

    const q2 = td.quantile(0.9);
    expect(q2).toBe(q1);
  });

  test('Odd inputs', () => {
    const td = new TDigest();
    td.add(NaN, 1);
    td.add(1, NaN);
    td.add(1, 0);
    td.add(1, -1000);
    expect(td.count()).toBe(0);

    // Infinite values are allowed.
    td.add(1, 1);
    td.add(2, 1);
    td.add(Infinity, 1);
    expect(td.quantile(0.5)).toBe(2);
    expect(td.quantile(0.9)).toBe(NaN);
  });

  test('merge', () => {
    // Repeat merges enough times to ensure we call compress()
    const numRepeats = 20;
    const addDigest = new TDigest();
    for (let i = 0; i < numRepeats; i++) {
      for (const c of normalDigest.centroids()) {
        addDigest.addCentroid(c);
      }
      for (const c of uniformDigest.centroids()) {
        addDigest.addCentroid(c);
      }
    }

    const mergeDigest = new TDigest();
    for (let i = 0; i < numRepeats; i++) {
      mergeDigest.merge(normalDigest);
      mergeDigest.merge(uniformDigest);
    }

    expect(() => compareQuantiles(addDigest, mergeDigest, 0.001)).not.toThrow();

    // Empty merge does nothing and has no effect on underlying centroids.
    const c1 = addDigest.centroids();
    addDigest.merge(new TDigest());
    const c2 = addDigest.centroids();
    expect(c2).toEqual(c1);
  });

  describe('Centroids', () => {
    const tests = [
      {
        name: 'increasing',
        data: [1, 2, 3, 4, 5],
        want: [
          new Centroid(1.0, 1.0),
          new Centroid(2.5, 2.0),
          new Centroid(4.0, 1.0),
          new Centroid(5.0, 1.0),
        ],
      },
    ];

    test.each(tests)('$name', ({data, want}) => {
      const td = new TDigest(3);
      for (const x of data) {
        td.add(x, 1);
      }
      const got = td.centroids();
      expect(got).toEqual(want);
    });
  });
});
