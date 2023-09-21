import {describe, expect, test} from '@jest/globals';
import {
  isValidSubdomain,
  sanitizeForSubdomain,
  sanitizeForLabel,
} from './team.js';

describe('team subdomain validation', () => {
  type Case = {
    desc: string;
    name: string;
    valid?: boolean;
    subdomain?: string;
    label?: string;
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
      label: 'thisis1validname0',
    },
    {
      desc: 'cannot be uppercase',
      name: 'NotAValidName',
      subdomain: 'notavalidname',
      label: 'notavalidname',
    },
    {
      desc: 'cannot start with digit',
      name: '0is-not-allowed',
      subdomain: 'is-not-allowed',
      label: 'isnotallowed',
    },
    {
      desc: 'dots not allowed',
      name: 'foo.bar',
      subdomain: 'foo-bar',
      label: 'foobar',
    },
    {
      desc: 'starting and trailing illegal characters',
      name: '.foo.bar.',
      subdomain: 'foo-bar',
      label: 'foobar',
    },
    {
      desc: 'consecutive illegal characters coalesced',
      name: 'My Company, LLC.',
      subdomain: 'my-company-llc',
      label: 'mycompanyllc',
    },
    {
      desc: 'cannot end with hyphen',
      name: 'cannot-end-with-hyphen-',
      subdomain: 'cannot-end-with-hyphen',
      label: 'cannotendwithhyphen',
    },
    {
      desc: 'cannot have spaces',
      name: 'name with spaces',
      subdomain: 'name-with-spaces',
      label: 'namewithspaces',
    },
    {
      desc: 'space in the beginning',
      name: ' name-starting-with-space',
      subdomain: 'name-starting-with-space',
      label: 'namestartingwithspace',
    },
    {
      desc: 'space at the end',
      name: 'name-ending-with-space ',
      subdomain: 'name-ending-with-space',
      label: 'nameendingwithspace',
    },
  ];

  for (const c of cases) {
    test(c.desc, () => {
      expect(isValidSubdomain(c.name)).toBe(c.valid ?? false);
      if (c.subdomain) {
        expect(sanitizeForSubdomain(c.name)).toBe(c.subdomain);
      } else {
        expect(sanitizeForSubdomain(c.name)).toBe(c.name);
      }
      if (c.label) {
        expect(sanitizeForLabel(c.name)).toBe(c.label);
      } else {
        expect(sanitizeForLabel(c.name)).toBe(c.name);
      }
    });
  }
});
