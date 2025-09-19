import {defineConfig} from 'vite';
import react from '@vitejs/plugin-react';

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  define: {
    'process.env.REPLICACHE_VERSION': JSON.stringify('1.2.3'),
  },
});
