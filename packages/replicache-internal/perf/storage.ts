import {uuid} from '../src/uuid';
import {randomString} from './data';
import type {Benchmark} from './perf';

export function benchmarks(): Benchmark[] {
  return [localStorageRead(), localStorageWrite()];
}

function localStorageRead() {
  return {
    name: 'localStorage read',
    group: 'storage',
    key: '',
    value: <string | null>null,
    async setup() {
      this.key = uuid();
      localStorage.setItem(this.key, randomString(100));
    },
    async teardown() {
      localStorage.removeItem(this.key);
    },
    async run() {
      // Assign to ensure this read isn't optimized away.
      this.value = localStorage.getItem(this.key);
    },
  };
}

function localStorageWrite() {
  return {
    name: 'localStorage write',
    group: 'storage',
    key: '',
    value: '',
    async setup() {
      this.key = uuid();
      this.value = randomString(100);
    },
    async teardown() {
      localStorage.removeItem(this.key);
    },
    async run() {
      localStorage.setItem(this.key, this.value);
    },
  };
}
