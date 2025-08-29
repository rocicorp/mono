import react from '@vitejs/plugin-react';
import {defineConfig} from 'vite';

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    target: 'esnext',
    rollupOptions: {
      external: ['expo-sqlite'],
    },
  },
  resolve: {
    alias: {
      // Mock expo-sqlite to prevent eager transformation by Vitest
      // Even though it's loaded via dynamic import, Vitest eagerly transforms it during dependency analysis
      'expo-sqlite': 'data:text/javascript,export default {}',
    },
  },
});
