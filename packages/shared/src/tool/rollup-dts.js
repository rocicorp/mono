/* eslint-env node */

import {nodeResolve} from '@rollup/plugin-node-resolve';
import dts from 'rollup-plugin-dts';
import tsConfigPaths from 'rollup-plugin-tsconfig-paths';

// We only use rollup for creating a bundled d.ts file.
// We use esbuild for building the actual code.

/**
 * @param {import('rollup').InputOption} input
 * @param {import('rollup').OutputOptions | string} output
 * @returns {import('rollup').RollupOptions}
 */
export function makeRollupOptions(input, output) {
  if (typeof output === 'string') {
    output = {file: output};
  }
  return {
    input,
    output,
    external: ['@rocicorp/lock', '@rocicorp/logger', '@rocicorp/resolver'],
    plugins: [
      tsConfigPaths(),
      nodeResolve(),
      dts({
        respectExternal: true,
      }),
    ],
  };
}
