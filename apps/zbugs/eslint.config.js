import baseConfig from '../../eslint.config.js';

export default [
  ...baseConfig,
  {
    files: ['**/*.ts', '**/*.tsx', '**/*.js', '**/*.jsx'],
    rules: {
      '@typescript-eslint/restrict-template-expressions': 'off',
      '@typescript-eslint/no-floating-promises': 'off',
      '@typescript-eslint/naming-convention': 'off',
      '@typescript-eslint/no-unsafe-return': 'off',
      '@typescript-eslint/no-misused-promises': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unnecessary-type-assertion': 'off',
      '@typescript-eslint/prefer-promise-reject-errors': 'off',
      '@typescript-eslint/restrict-plus-operands': 'off',
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/await-thenable': 'off',
      'no-console': 'off',
      'object-shorthand': 'off',
      'prefer-destructuring': 'off',
      'prefer-arrow-callback': 'off',
      'arrow-body-style': 'off',
      'eqeqeq': 'off',
      'no-restricted-imports': 'off',
      'react-hooks/exhaustive-deps': 'off',
    },
  },
];