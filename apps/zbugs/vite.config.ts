import react from '@vitejs/plugin-react';
import {makeDefine} from 'shared/src/build.js';
import {defineConfig} from 'vite';
import svgr from 'vite-plugin-svgr';
import tsconfigPaths from 'vite-tsconfig-paths';

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [tsconfigPaths(), svgr(), react()],
  define: makeDefine(),
  build: {
    target: 'esnext',
  },
});
