import react from '@vitejs/plugin-react';
import {defineConfig} from 'vite';

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  define: {
    'process.env.REPLICACHE_VERSION': JSON.stringify('1.2.3'),
  },
});
