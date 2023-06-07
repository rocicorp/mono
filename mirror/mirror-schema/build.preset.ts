import {definePreset} from 'unbuild';

// @see https://github.com/unjs/unbuild
export default definePreset({
  clean: true,
  rollup: {
    emitCJS: true,
    inlineDependencies: true,
    esbuild: {
      minify: false,
    },
  },
});
