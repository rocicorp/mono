import {makeRollupOptions} from 'shared/src/tool/rollup-dts.js';

export default makeRollupOptions(
  {
    replicache: 'out/.dts/replicache/src/mod.d.ts',
    impl: 'out/.dts/replicache/src/replicache-impl.d.ts',
  },
  {
    dir: 'out/',
    chunkFileNames: 'chunk-[hash].d.ts',
  },
);
