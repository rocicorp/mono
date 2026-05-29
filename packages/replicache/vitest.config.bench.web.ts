import {benchConfig} from 'shared/src/tool/vitest-config.ts';
import {mergeConfig} from 'vitest/config';

export default mergeConfig(benchConfig, {
  test: {
    name: 'replicache/bench/browser',
    benchmark: {
      exclude: ['src/**/*.{bench,benchmark}.node.?(c|m)[jt]s?(x)'],
    },
  },
});
