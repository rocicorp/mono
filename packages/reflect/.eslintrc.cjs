module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  plugins: ['@typescript-eslint'],
  extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended'],
  parserOptions: {
    project: './tsconfig.json',
  },
  rules: {
    '@typescript-eslint/no-floating-promises': 'error',
    '@typescript-eslint/naming-convention': [
      'error',
      {
        selector: 'memberLike',
        modifiers: ['public'],
        format: ['camelCase'],
        leadingUnderscore: 'forbid',
      },
    ],
    'eqeqeq': 'error',
    'no-var': 'error',
    'object-shorthand': 'error',
    'prefer-arrow-callback': 'error',
    'prefer-destructuring': [
      'error',
      {
        VariableDeclarator: {
          object: true,
        },
      },
      {
        enforceForRenamedProperties: false,
      },
    ],
    'no-unused-vars': 'off',
    '@typescript-eslint/no-unused-vars': ['error', {argsIgnorePattern: '^_'}],
  },
};
