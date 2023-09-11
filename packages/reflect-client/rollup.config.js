/* eslint-env node */

import {nodeResolve} from '@rollup/plugin-node-resolve';
import dts from 'rollup-plugin-dts';

// We only use rollup for creating a bundled d.ts file.
// We use esbuild for building the actual code.

/** @type {import('rollup').InputOptions} */
export default {
  input: 'out/.dts/mod.d.ts',
  output: {
    file: `./out/reflect-client.d.ts`,
  },
  external: [
    '@rocicorp/lock',
    '@rocicorp/logger',
    '@rocicorp/resolver',
    'replicache',
  ],
  plugins: [
    nodeResolve(),
    dts({
      respectExternal: true,
    }),
  ],
};
