import solid from 'vite-plugin-solid';
import {defineConfig} from 'vite';
import tsconfigPaths from 'vite-tsconfig-paths';
import {makeDefine} from '../../packages/shared/src/build.ts';

export default defineConfig({
  plugins: [tsconfigPaths(), solid()],
  define: makeDefine(),
  build: {
    target: 'esnext',
  },
});
