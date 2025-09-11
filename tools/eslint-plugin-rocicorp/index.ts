/**
 * @fileoverview Custom ESLint rules for Rocicorp
 * @author Rocicorp
 */

import { noUnhandledQuery } from './rules/no-unhandled-query';

export const rules = {
  'no-unhandled-query': noUnhandledQuery,
};

export const configs = {
  recommended: {
    plugins: ['rocicorp'],
    rules: {
      'rocicorp/no-unhandled-query': 'error',
    },
  },
};