import {describe, expect, test} from '@jest/globals';
import {epochMicrosToTimestampTz} from './big-time.js';

describe('types/bigtime', () => {
  type Case = {
    name: string;
    micros: bigint;
    tz: string;
  };

  const cases: Case[] = [
    {
      name: 'simple',
      micros: 1711047023646716n,
      tz: '2024-03-21T18:50:23.646716Z',
    },
    {
      name: 'leading zeros',
      micros: 1711047023646006n,
      tz: '2024-03-21T18:50:23.646006Z',
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      expect(epochMicrosToTimestampTz(c.micros)).toBe(c.tz);
    });
  }
});
