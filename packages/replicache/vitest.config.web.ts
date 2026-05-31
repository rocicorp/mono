import {mergeConfig} from 'vitest/config';
import {newConfig} from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(newConfig(), {
  test: {
    name: 'replicache/browser',
    exclude: ['src/**/*.{test,spec}.node.?(c|m)[jt]s?(x)'],
    benchmark: {
      exclude: ['src/**/*.{bench,benchmark}.node.?(c|m)[jt]s?(x)'],
    },
  },
});
