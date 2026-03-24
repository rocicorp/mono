import {compareUTF8} from 'compare-utf8';
import fc from 'fast-check';
import {describe, expect, test} from 'vitest';
import {
  compareStringUTF8Fast,
  compareValues,
  makeComparator,
  normalizeUndefined,
  valuesEqual,
} from './data.ts';
import {compareRowsTest} from './test/compare-rows-test.ts';

test('normalizeUndefined', () => {
  fc.assert(
    fc.property(fc.constantFrom(null, undefined), v => {
      expect(normalizeUndefined(v)).toBe(null);
    }),
  );
  fc.assert(
    fc.property(fc.oneof(fc.boolean(), fc.double(), fc.string()), b => {
      expect(normalizeUndefined(b)).toBe(b);
    }),
  );
});

test('compareValues', () => {
  // null and undefined are equal to each other
  fc.assert(
    fc.property(
      fc.constantFrom(null, undefined),
      fc.constantFrom(null, undefined),
      (v1, v2) => {
        expect(compareValues(v1, v2)).toBe(0);
      },
    ),
  );

  // null and undefined are less than any other value
  fc.assert(
    fc.property(
      fc.constantFrom(null, undefined),
      fc.oneof(fc.boolean(), fc.double(), fc.fullUnicodeString()),
      (v1, v2) => {
        expect(compareValues(v1, v2)).lessThan(0);
        expect(compareValues(v2, v1)).greaterThan(0);
      },
    ),
  );

  // boolean
  fc.assert(
    fc.property(fc.boolean(), fc.boolean(), (b1, b2) => {
      expect(compareValues(b1, b2)).toBe(b1 === b2 ? 0 : b1 ? 1 : -1);
    }),
  );
  fc.assert(
    fc.property(
      fc.boolean(),
      fc.oneof(fc.double(), fc.fullUnicodeString()),
      (b, v) => {
        expect(() => compareValues(b, v)).toThrow('expected boolean');
      },
    ),
  );

  // number
  fc.assert(
    fc.property(fc.double(), fc.double(), (n1, n2) => {
      // compareValues uses === so `0` and `-0` are same.
      // toBe uses Object.is so 0 and -0 are different.
      // normalize -0 to 0 for this test.
      if (n1 === 0) n1 = 0;
      if (n2 === 0) n2 = 0;

      expect(compareValues(n1, n2)).toBe(n1 - n2);
    }),
  );
  fc.assert(
    fc.property(
      fc.double(),
      fc.oneof(fc.boolean(), fc.fullUnicodeString()),
      (n, v) => {
        expect(() => compareValues(n, v)).toThrow('expected number');
      },
    ),
  );

  // string - compareStringUTF8Fast returns different magnitudes for ASCII
  // but always matches the sign of compareUTF8
  fc.assert(
    fc.property(fc.fullUnicodeString(), fc.fullUnicodeString(), (s1, s2) => {
      expect(Math.sign(compareValues(s1, s2))).toBe(
        Math.sign(compareUTF8(s1, s2)),
      );
    }),
  );
  fc.assert(
    fc.property(
      fc.fullUnicodeString(),
      fc.oneof(fc.boolean(), fc.double()),
      (s, v) => {
        expect(() => compareValues(s, v)).toThrow('expected string');
      },
    ),
  );
});

test('valuesEquals', () => {
  fc.assert(
    fc.property(
      fc.oneof(fc.boolean(), fc.double(), fc.fullUnicodeString()),
      fc.oneof(fc.boolean(), fc.double(), fc.fullUnicodeString()),
      (v1, v2) => {
        expect(valuesEqual(v1, v2)).toBe(v1 === v2);
      },
    ),
  );

  fc.assert(
    fc.property(
      fc.constantFrom(null, undefined),
      fc.oneof(
        fc.constantFrom(null, undefined),
        fc.boolean(),
        fc.double(),
        fc.fullUnicodeString(),
      ),
      (v1, v2) => {
        expect(valuesEqual(v1, v2)).false;
        expect(valuesEqual(v2, v1)).false;
      },
    ),
  );
});

test('comparator', () => {
  compareRowsTest(makeComparator);
});

describe('compareStringUTF8Fast', () => {
  test('ASCII strings compare correctly', () => {
    expect(compareStringUTF8Fast('abc', 'def')).toBeLessThan(0);
    expect(compareStringUTF8Fast('def', 'abc')).toBeGreaterThan(0);
    expect(compareStringUTF8Fast('abc', 'abc')).toBe(0);
  });

  test('empty strings', () => {
    expect(compareStringUTF8Fast('', '')).toBe(0);
    expect(compareStringUTF8Fast('', 'a')).toBeLessThan(0);
    expect(compareStringUTF8Fast('a', '')).toBeGreaterThan(0);
  });

  test('Unicode strings fall back correctly', () => {
    // Non-ASCII chars trigger compareUTF8 fallback; sign must match
    expect(Math.sign(compareStringUTF8Fast('café', 'cafë'))).toBe(
      Math.sign(compareUTF8('café', 'cafë')),
    );
  });

  test('prefix strings', () => {
    expect(compareStringUTF8Fast('abc', 'abcd')).toBeLessThan(0);
    expect(compareStringUTF8Fast('abcd', 'abc')).toBeGreaterThan(0);
  });

  test('sign matches compareUTF8 for all ASCII', () => {
    fc.assert(
      fc.property(fc.asciiString(), fc.asciiString(), (a, b) => {
        expect(Math.sign(compareStringUTF8Fast(a, b))).toBe(
          Math.sign(compareUTF8(a, b)),
        );
      }),
    );
  });
});

describe('makeComparator single-key fast path', () => {
  test('single key asc matches multi-key behavior', () => {
    const singleKey = makeComparator([['name', 'asc']]);
    const multiKey = makeComparator([
      ['name', 'asc'],
      ['id', 'asc'],
    ]);
    // For rows where only 'name' differs, both should give same sign
    expect(Math.sign(singleKey({name: 'a'}, {name: 'b'}))).toBe(
      Math.sign(multiKey({name: 'a', id: '1'}, {name: 'b', id: '1'})),
    );
  });

  test('single key desc', () => {
    const cmp = makeComparator([['name', 'desc']]);
    expect(cmp({name: 'a'}, {name: 'b'})).toBeGreaterThan(0);
  });

  test('single key with reverse', () => {
    const cmp = makeComparator([['name', 'asc']], true);
    expect(cmp({name: 'a'}, {name: 'b'})).toBeGreaterThan(0);
  });

  test('single key desc with reverse', () => {
    const cmp = makeComparator([['name', 'desc']], true);
    expect(cmp({name: 'a'}, {name: 'b'})).toBeLessThan(0);
  });

  test('single key equality', () => {
    const cmp = makeComparator([['id', 'asc']]);
    expect(cmp({id: 42}, {id: 42})).toBe(0);
  });
});
