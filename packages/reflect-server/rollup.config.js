/* eslint-env node */

import dts from 'rollup-plugin-dts';

// We only use rollup for creating a bundled d.ts file.
// We use esbuild for building the actual code.

export default {
  input: 'out/.dts/src/mod.d.ts',
  output: {
    file: `./out/reflect-server.d.ts`,
  },
  plugins: [
    dts({
      respectExternal: true,
    }),
  ],
};
