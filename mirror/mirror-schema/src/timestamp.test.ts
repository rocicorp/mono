import {describe, expect, test} from '@jest/globals';
import {toMillis} from './timestamp.js';

describe('timestamp', () => {
  test('toMillis', () => {
    type Case = {
      seconds?: number;
      nanos?: number;
      millis: number;
    };

    const cases: Case[] = [
      {
        seconds: 123,
        nanos: 93829489,
        millis: 123094,
      },
      {
        seconds: 123,
        nanos: 93429489,
        millis: 123093,
      },
      {
        seconds: 123,
        nanos: 0,
        millis: 123000,
      },
      {
        seconds: 0,
        nanos: 489000,
        millis: 0,
      },
      {
        seconds: 0,
        nanos: 589000,
        millis: 1,
      },
    ];

    for (const c of cases) {
      expect(
        toMillis({seconds: c.seconds ?? 0, nanoseconds: c.nanos ?? 0}),
      ).toBe(c.millis);
    }
  });
});
