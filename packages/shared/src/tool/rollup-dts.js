/* eslint-env node */

import {nodeResolve} from '@rollup/plugin-node-resolve';
import dts from 'rollup-plugin-dts';
import tsConfigPaths from 'rollup-plugin-tsconfig-paths';

// We only use rollup for creating a bundled d.ts file.
// We use esbuild for building the actual code.

/**
 * @param {string} input
 * @param {string} outputFile
 * @returns {import('rollup').InputOptions}
 */
export function makeInputOptions(input, outputFile) {
  /** @type {import('rollup').InputOptions} */
  const config = {
    input,
    output: {
      file: outputFile,
    },
    external: ['@rocicorp/lock', '@rocicorp/logger', '@rocicorp/resolver'],
    plugins: [
      tsConfigPaths(),
      nodeResolve(),
      dts({
        respectExternal: true,
      }),
    ],
  };
  return config;
}
