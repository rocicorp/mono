import type {Benchmark} from './perf.js';

export function benchmarks(): Array<Benchmark> {
  return [forLoop(), forEach()];
}

const COUNT = 1_000;

const m = new Map(Array.from({length: COUNT}, (_, i) => [i, i]));

function forLoop(): Benchmark {
  return {
    name: 'map for loop',
    group: 'map-loop',
    run() {
      let sum = 0;
      for (let i = 0; i < COUNT; i++) {
        for (const [key, value] of m) {
          sum += key + value;
        }
      }
      console.log(sum);
    },
  };
}

function forEach(): Benchmark {
  return {
    name: 'map forEach',
    group: 'map-loop',
    run() {
      let sum = 0;
      for (let i = 0; i < COUNT; i++) {
        m.forEach((value, key) => {
          sum += key + value;
        });
      }
      console.log(sum);
    },
  };
}
