import {mergeConfig} from 'vitest/config';
import config from '../shared/src/tool/vitest-config.ts';

// Vectors are written to disk, so the generators run in Node rather than in
// the browser like most zero-client tests.
export default mergeConfig(config, {
  test: {
    name: 'zero-conformance',
    browser: {
      enabled: false,
    },
  },
});
