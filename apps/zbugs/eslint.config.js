import rootConfig from '../../eslint.config.js';
import reactRefresh from 'eslint-plugin-react-refresh';

export default [
  ...rootConfig,
  {
    plugins: {
      'react-refresh': reactRefresh,
    },
    rules: {
      'react-refresh/only-export-components': [
        'warn',
        {allowConstantExport: true},
      ],
    },
  },
];
