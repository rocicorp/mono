/**
 * @fileoverview Custom ESLint rules for Rocicorp
 * @author Rocicorp
 */

'use strict';

module.exports = {
  rules: {
    'no-unhandled-query': require('./rules/no-unhandled-query'),
  },
  configs: {
    recommended: {
      plugins: ['rocicorp'],
      rules: {
        'rocicorp/no-unhandled-query': 'error',
      },
    },
  },
};