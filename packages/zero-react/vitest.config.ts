import {resolve} from 'node:path';
import {defineConfig, mergeConfig} from 'vitest/config';
import baseConfig from '../shared/src/tool/vitest-config.ts';

export default mergeConfig(
  baseConfig,
  defineConfig({
    resolve: {
      alias: {
        'react': resolve(__dirname, 'node_modules/react'),
        'react-dom': resolve(__dirname, 'node_modules/react-dom'),
      },
    },
  }),
);
