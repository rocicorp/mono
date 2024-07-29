import {describe, expect, test} from '@jest/globals';
import {
  variableIsWithinSizeLimit,
  variableNameIsWithinSizeLimit,
} from './vars.js';

describe('vars name size limit', () => {
  type Case = {
    name: string;
    key: string;
    expected: boolean;
  };

  const cases: Case[] = [
    {
      name: 'ascii <= 1k',
      key: 'A'.repeat(1024),
      expected: true,
    },
    {
      name: 'ascii > 1k',
      key: 'A'.repeat(1025),
      expected: false,
    },
    {
      name: 'non-ascii latin1 <= 1k', // 2-byte unicode characters
      key: '£'.repeat(512),
      expected: true,
    },
    {
      name: 'non-ascii latin1 > 1k', // 2-byte unicode characters
      key: '£'.repeat(513),
      expected: false,
    },
    {
      name: 'CJK <= 1k', // 3-byte unicode characters
      key: '中'.repeat(341),
      expected: true,
    },
    {
      name: 'CJK > 1k', // 3-byte unicode characters
      key: '中'.repeat(342),
      expected: false,
    },
    {
      name: 'Emoji <= 1k', // 4-byte unicode characters
      key: '😁'.repeat(256),
      expected: true,
    },
    {
      name: 'Emoji > 1k', // 4-byte unicode characters
      key: '😁'.repeat(257),
      expected: false,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      expect(variableNameIsWithinSizeLimit(c.key)).toBe(c.expected);
    });
  }
});

describe('vars size limit', () => {
  type Case = {
    name: string;
    key: string;
    value: string;
    expected: boolean;
  };

  const cases: Case[] = [
    {
      name: 'ascii <= 5k',
      key: 'A'.repeat(1024),
      value: 'B'.repeat(4096),
      expected: true,
    },
    {
      name: 'ascii > 5k',
      key: 'A'.repeat(1024),
      value: 'B'.repeat(4097),
      expected: false,
    },
    {
      name: 'non-ascii latin1 <= 5k', // 2-byte unicode characters
      key: '£'.repeat(512),
      value: 'И'.repeat(2048),
      expected: true,
    },
    {
      name: 'non-ascii latin1 > 5k', // 2-byte unicode characters
      key: '£'.repeat(512),
      value: 'И'.repeat(2049),
      expected: false,
    },
    {
      name: 'CJK <= 5k', // 3-byte unicode characters
      key: '中'.repeat(706),
      value: '文'.repeat(1000),
      expected: true,
    },
    {
      name: 'CJK > 5k', // 3-byte unicode characters
      key: '中'.repeat(706),
      value: '文'.repeat(1001),
      expected: false,
    },
    {
      name: 'Emoji <= 5k', // 4-byte unicode characters
      key: '😁'.repeat(256),
      value: '😜'.repeat(1024),
      expected: true,
    },
    {
      name: 'Emoji > 5k', // 4-byte unicode characters
      key: '😁'.repeat(256),
      value: '😜'.repeat(1025),
      expected: false,
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      expect(variableIsWithinSizeLimit(c.key, c.value)).toBe(c.expected);
    });
  }
});
