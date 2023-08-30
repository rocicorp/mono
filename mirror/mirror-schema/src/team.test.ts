import {describe, expect, test} from '@jest/globals';
import {isValidSubdomain, sanitizeForSubdomain} from './team.js';

describe('team subdomain validation', () => {
  type Case = {
    desc: string;
    name: string;
    valid?: boolean;
    sanitized?: string;
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
      sanitized: 'notavalidname',
    },
    {
      desc: 'cannot start with digit',
      name: '0is-not-allowed',
      sanitized: 'is-not-allowed',
    },
    {
      desc: 'dots not allowed',
      name: 'foo.bar',
      sanitized: 'foo-bar',
    },
    {
      desc: 'starting and trailing illegal characters',
      name: '.foo.bar.',
      sanitized: 'foo-bar',
    },
    {
      desc: 'consecutive illegal characters coalesced',
      name: 'My Company, LLC.',
      sanitized: 'my-company-llc',
    },
    {
      desc: 'cannot end with hyphen',
      name: 'cannot-end-with-hyphen-',
      sanitized: 'cannot-end-with-hyphen',
    },
    {
      desc: 'cannot have spaces',
      name: 'name with spaces',
      sanitized: 'name-with-spaces',
    },
    {
      desc: 'space in the beginning',
      name: ' name-starting-with-space',
      sanitized: 'name-starting-with-space',
    },
    {
      desc: 'space at the end',
      name: 'name-ending-with-space ',
      sanitized: 'name-ending-with-space',
    },
  ];

  for (const c of cases) {
    test(c.desc, () => {
      expect(isValidSubdomain(c.name)).toBe(c.valid ?? false);
      if (c.sanitized) {
        expect(sanitizeForSubdomain(c.name)).toBe(c.sanitized);
      } else {
        expect(sanitizeForSubdomain(c.name)).toBe(c.name);
      }
    });
  }
});
