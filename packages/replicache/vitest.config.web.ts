import config from 'shared/src/tool/vitest-config.ts';
import {mergeConfig} from 'vitest/config';

export default mergeConfig(config, {
  test: {
    name: 'replicache/browser',
    exclude: ['src/**/*.{test,spec}.node.?(c|m)[jt]s?(x)'],
    benchmark: {
      exclude: ['src/**/*.{bench,benchmark}.node.?(c|m)[jt]s?(x)'],
    },
  },
});
