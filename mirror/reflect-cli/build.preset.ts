import {definePreset} from 'unbuild';

// @see https://github.com/unjs/unbuild
export default definePreset({
  clean: true,
  rollup: {
    inlineDependencies: true,
    esbuild: {
      minify: true,
    },
  },
});
