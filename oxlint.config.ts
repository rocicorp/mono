import {defineConfig} from 'oxlint';
import {baseConfig} from './oxlint.base.ts';

export default defineConfig({
  ...baseConfig,
  options: {
    typeAware: true,
    reportUnusedDisableDirectives: 'error',
  },
});
