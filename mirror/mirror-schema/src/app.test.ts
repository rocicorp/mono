import {describe, expect, test} from '@jest/globals';
import {isValidAppName} from './app.js';

describe('app name validation', () => {
  type Case = {
    desc: string;
    name: string;
    valid?: boolean;
  };
  const cases: Case[] = [
    {
      desc: 'alphanumeric',
      name: 'valid0name0',
      valid: true,
    },
    {
      desc: 'alphanumeric with hyphens',
      name: 'this-is-1-valid-name0',
      valid: true,
    },
    {
      desc: 'cannot be uppercase',
      name: 'NotAValidName',
    },
    {
      desc: 'cannot start with digit',
      name: '0is-not-allowed',
    },
    {
      desc: 'cannot end with hyphen',
      name: 'cannot-end-with-hyphen-',
    },
    {
      desc: 'cannot have spaces',
      name: 'name with spaces',
    },
    {
      desc: 'space in the beginning',
      name: ' name-starting-with-space',
    },
    {
      desc: 'space at the end',
      name: 'name-ending-with-space ',
    },
  ];

  for (const c of cases) {
    test(c.desc, () => {
      expect(isValidAppName(c.name)).toBe(c.valid ?? false);
    });
  }
});
