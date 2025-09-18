import {bench, describe} from 'vitest';
import {
  jsonArrayTestData,
  jsonObjectTestData,
  randomString,
} from '../../shared/src/test-data.ts';
import {getSizeOfValue} from './size-of-value.ts';

describe('getSizeOfValue performance', () => {
  // Core primitive types - essential benchmarks
  describe('primitives', () => {
    bench('string (100 chars)', () => {
      getSizeOfValue(randomString(100));
    });

    bench('integer', () => {
      getSizeOfValue(42);
    });

    bench('boolean', () => {
      getSizeOfValue(true);
    });

    bench('null', () => {
      getSizeOfValue(null);
    });
  });

  // Essential array tests
  describe('arrays', () => {
    const smallArray = Array.from({length: 10}, (_, i) => `item${i}`);
    const largeArray = Array.from({length: 100}, (_, i) => `item${i}`);

    bench('small array (10 items)', () => {
      getSizeOfValue(smallArray);
    });

    bench('large array (100 items)', () => {
      getSizeOfValue(largeArray);
    });
  });

  // Essential object tests - focus on replicache-style data
  describe('objects', () => {
    const testDataSmall = jsonObjectTestData(256);
    const testDataLarge = jsonObjectTestData(1024);

    bench('structured object (256B)', () => {
      getSizeOfValue(testDataSmall);
    });

    bench('structured object (1KB)', () => {
      getSizeOfValue(testDataLarge);
    });

    // One complex nested structure
    const nestedObject = {
      data: randomString(50),
      nested: {
        values: [1, 2, 3],
        info: {str: 'test', num: 42},
      },
    };

    bench('nested object', () => {
      getSizeOfValue(nestedObject);
    });
  });

  // Essential dataset tests - focus on realistic sizes
  describe('datasets', () => {
    const smallDataset = jsonArrayTestData(10, 256);
    const largeDataset = jsonArrayTestData(100, 512);

    bench('small dataset (10x256B)', () => {
      getSizeOfValue(smallDataset);
    });

    bench('large dataset (100x512B)', () => {
      getSizeOfValue(largeDataset);
    });
  });
});
