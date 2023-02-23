import type {Benchmark} from './perf.js';

import * as m from '../src/uuid.js';

export function benchmarks(): Array<Benchmark> {
  return [uuidNative(), uuidNoNative()];
}

const COUNT = 10_000;

function uuidNoNative(): Benchmark {
  return {
    name: 'uuid no native',
    group: 'uuid',
    run() {
      for (let i = 0; i < COUNT; i++) {
        m.uuidNoNative();
      }
    },
  };
}

function uuidNative(): Benchmark {
  return {
    name: 'uuid native',
    group: 'uuid',
    run() {
      for (let i = 0; i < COUNT; i++) {
        m.uuidNative();
      }
    },
  };
}
