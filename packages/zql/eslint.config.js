import eslintConfig from '@rocicorp/eslint-config';

export default [
  ...eslintConfig,
  {
    ignores: [
      'node_modules/',
      'out/',
      '.eslintrc.cjs',
      'vitest.config.ts',
    ],
  },
];