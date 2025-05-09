/* eslint-disable @typescript-eslint/no-explicit-any */
import {expect} from 'vitest';

/**
 * This matcher has the downside of covering up errors where `null` is expected but `undefined` is returned.
 *
 *  `one` relationships will be returned by postgres as `null` vs zql will return them as `undefined`.
 *
 * This also removes the `Symbol(rc)` fields from the `received` ZQL view as `PG` will not set them.
 *
 * Maybe we should change this in zql. Til then...
 *
 */
expect.extend({
  toEqualPg(received: any, expected: any) {
    const normalize = (obj: any): any => {
      if (obj === null || obj === undefined) {
        return null;
      }

      if (typeof obj !== 'object' || obj === null) {
        return obj;
      }

      if (Array.isArray(obj)) {
        return obj.map(normalize);
      }

      return Object.fromEntries(
        Object.entries(obj).map(([key, value]) => [key, normalize(value)]),
      );
    };

    const normalizedReceived = normalize(received);
    const normalizedExpected = normalize(expected);

    const pass = this.equals(normalizedReceived, normalizedExpected);

    return {
      pass,
      message: () =>
        pass
          ? `Expected ${received} not to equal ${expected} (treating null/undefined as equal)`
          : `Expected ${received} to equal ${expected} (treating null/undefined as equal)`,
      actual: received,
      expected,
    };
  },
});

// You'll need to extend the Vitest types
declare module 'vitest' {
  interface Assertion {
    toEqualPg(expected: any): void;
  }
}
