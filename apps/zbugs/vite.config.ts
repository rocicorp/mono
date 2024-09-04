import {defineConfig} from 'vite';
import react from '@vitejs/plugin-react';
import tsconfigPaths from 'vite-tsconfig-paths';
import {makeDefine} from 'shared/src/build.js';

const ReactCompilerConfig = {};

// https://vitejs.dev/config/
export default defineConfig({
  define: makeDefine(),
  plugins: [
    tsconfigPaths(),
    react({
      babel: {
        plugins: [['babel-plugin-react-compiler', ReactCompilerConfig]],
      },
    }),
  ],
  build: {
    target: 'esnext',
  },
});
