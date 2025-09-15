import rocicorpConfig from '@rocicorp/eslint-config';

export default [
  ...rocicorpConfig,
  {
    rules: {
      // Disable these rules to allow explicit undefined in optional parameters as per project convention
      '@typescript-eslint/no-duplicate-type-constituents': 'off',
      '@typescript-eslint/no-redundant-type-constituents': 'off',
      // Only disable rules that have very high frequency (100+ errors) in the codebase
      '@typescript-eslint/no-unsafe-assignment': 'off',
      '@typescript-eslint/no-unsafe-return': 'off', 
      '@typescript-eslint/no-unsafe-member-access': 'off',
      '@typescript-eslint/no-unsafe-call': 'off',
      '@typescript-eslint/no-unsafe-argument': 'off',
      '@typescript-eslint/restrict-template-expressions': 'off',
      '@typescript-eslint/no-base-to-string': 'off',
      '@typescript-eslint/no-unused-expressions': 'off', // TODO: Enable after fixing test patterns
      '@typescript-eslint/no-unused-vars': 'error',
      '@typescript-eslint/unbound-method': 'off',
      '@typescript-eslint/no-unnecessary-type-assertion': 'error',
      '@typescript-eslint/no-floating-promises': 'error',
      '@typescript-eslint/no-misused-promises': 'off', // TODO: Enable after fixing Promise handlers
      '@typescript-eslint/naming-convention': 'error',
      '@typescript-eslint/prefer-promise-reject-errors': 'off',
      '@typescript-eslint/restrict-plus-operands': 'off',
      '@typescript-eslint/await-thenable': 'off',
      '@typescript-eslint/no-empty-object-type': 'off',
      '@typescript-eslint/only-throw-error': 'off',
      '@typescript-eslint/require-await': 'off', // TODO: Enable after fixing async functions
      'require-await': 'off', // TODO: Enable after fixing async functions
      'no-unused-private-class-members': 'error',
      'no-restricted-syntax': [
        'error',
        {
          selector: 'TSEnumDeclaration',
          message: 'Enums are not allowed. See shared/enum.ts',
        },
      ],
      'no-restricted-imports': [
        'error',
        {
          patterns: [
            {
              group: [
                'datadog/*',
                'otel/*',
                'replicache/*',
                'replicache-perf/*',
                'shared/*',
                'zero/*',
                'zero-cache/*',
                'zero-client/*',
                'zero-protocol/*',
                'zero-react/*',
                'zero-react-native/*',
                'zero-schema/*',
                'zero-solid/*',
                'zero-vue/*',
                'zql/*',
                'zqlite/*',
                'zqlite-zql-test/*',
              ],
              message: 'Use relative imports instead',
            },
            {
              group: ['**/mod.ts'],
              message: "Don't import from barrel files. Import from the specific module instead.",
            },
            {
              group: ['**/*.test.ts', '**/*.test.tsx'],
              message: 'Do not import from test files.',
            },
            {
              group: ['sinon'],
              message: 'Use vi instead of sinon',
            },
          ],
          paths: [
            {name: 'datadog', message: 'Use relative imports instead'},
            {name: 'otel', message: 'Use relative imports instead'},
            {name: 'replicache', message: 'Use relative imports instead'},
            {
              name: 'replicache-perf',
              message: 'Use relative imports instead',
            },
            {name: 'shared', message: 'Use relative imports instead'},
            {name: 'zero', message: 'Use relative imports instead'},
            {name: 'zero-cache', message: 'Use relative imports instead'},
            {name: 'zero-client', message: 'Use relative imports instead'},
            {name: 'zero-protocol', message: 'Use relative imports instead'},
            {name: 'zero-react', message: 'Use relative imports instead'},
            {
              name: 'zero-react-native',
              message: 'Use relative imports instead',
            },
            {name: 'zero-schema', message: 'Use relative imports instead'},
            {name: 'zero-solid', message: 'Use relative imports instead'},
            {name: 'zero-vue', message: 'Use relative imports instead'},
            {name: 'zql', message: 'Use relative imports instead'},
            {name: 'zqlite', message: 'Use relative imports instead'},
            {name: 'zqlite-zql-test', message: 'Use relative imports instead'},
          ],
        },
      ],
    },
  },
];