import type {Benchmark} from './perf';

import * as m from '../src/uuid';

export function benchmarks(): Array<Benchmark> {
  return [uuidNative(), uuidNoNative()];
}

const COUNT = 10_000;

function uuidNoNative(): Benchmark {
  return {
    name: 'uuid no native',
    group: 'uuid',
    async run() {
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
    async run() {
      for (let i = 0; i < COUNT; i++) {
        m.uuidNative();
      }
    },
  };
}
