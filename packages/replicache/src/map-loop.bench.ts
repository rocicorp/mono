import {bench, describe, use} from 'shared/src/bench.ts';

const COUNT = 1_000;

const m = new Map(Array.from({length: COUNT}, (_, i) => [i, i]));

describe('map-loop', () => {
  bench('map for loop', () => {
    let sum = 0;
    for (let i = 0; i < COUNT; i++) {
      for (const [key, value] of m) {
        sum += key + value;
      }
    }
    use(sum);
  });

  bench('map forEach', () => {
    let sum = 0;
    for (let i = 0; i < COUNT; i++) {
      m.forEach((value, key) => {
        sum += key + value;
      });
    }
    use(sum);
  });
});
