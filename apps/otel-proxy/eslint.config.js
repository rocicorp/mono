import eslintConfig from '@rocicorp/eslint-config';

export default [
  ...eslintConfig,
  {
    rules: {
      'no-restricted-imports': 'off',
    },
  },
];