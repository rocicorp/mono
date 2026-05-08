import {defineConfig} from 'oxlint';
import {baseConfig} from '../../oxlint.base.ts';

/**
 * Zbugs-specific oxlint configuration.
 * This is a nested config (relative to the mono root oxlint.config.ts),
 */
export default defineConfig({
  ...baseConfig,
  plugins: [...baseConfig.plugins, 'react'],
  rules: {
    ...baseConfig.rules,
    'react-hooks/rules-of-hooks': 'error',
    'react-hooks/exhaustive-deps': 'error',
  },
  env: {
    builtin: true,
    browser: true,
  },
});
