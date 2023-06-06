import {definePreset} from 'unbuild';

// @see https://github.com/unjs/unbuild
export default definePreset({
  clean: true,
  declaration: true,
  rollup: {
    externals: [
      'cors',
      'firebase-functions',
      'firebase-admin',
      'body-parser',
      'busboy',
      'shared',
    ],
    inlineDependencies: true,
    esbuild: {
      minify: false,
    },
  },
});
