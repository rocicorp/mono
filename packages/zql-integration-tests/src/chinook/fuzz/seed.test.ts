import {describe, expect, test} from 'vitest';
import {
  DEFAULT_SEED,
  formatSeed,
  fuzzBudget,
  fuzzSeed,
  reproHint,
} from './seed.ts';

describe('fuzzSeed', () => {
  test('defaults to the fixed per-PR seed when unset or blank', () => {
    expect(fuzzSeed({})).toBe(DEFAULT_SEED);
    expect(fuzzSeed({ZERO_FUZZ_SEED: ''})).toBe(DEFAULT_SEED);
    expect(fuzzSeed({ZERO_FUZZ_SEED: '  '})).toBe(DEFAULT_SEED);
  });

  test('reads decimal (a run number) and 0x hex (a printed seed)', () => {
    expect(fuzzSeed({ZERO_FUZZ_SEED: '1234'})).toBe(1234);
    expect(fuzzSeed({ZERO_FUZZ_SEED: '0x00c0ffee'})).toBe(0x00c0ffee);
    expect(fuzzSeed({ZERO_FUZZ_SEED: '0xffffffff'})).toBe(0xffffffff);
  });

  test('rejects a value it cannot replay exactly', () => {
    for (const bad of ['abc', '1.5', '-1', '0x100000000']) {
      expect(() => fuzzSeed({ZERO_FUZZ_SEED: bad})).toThrow(/ZERO_FUZZ_SEED/);
    }
  });

  test('rejects a misspelled fuzzer variable instead of ignoring it', () => {
    expect(() => fuzzSeed({ZERO_FUZZ_SEEED: '7'})).toThrow(
      /Unknown fuzzer variable ZERO_FUZZ_SEEED/,
    );
  });

  test('the replay hint prints the seed in the form fuzzSeed reads back', () => {
    const seed = fuzzSeed({ZERO_FUZZ_SEED: '48879'});
    expect(reproHint()).toContain(`ZERO_FUZZ_SEED=${formatSeed(seed)}`);
    expect(fuzzSeed({ZERO_FUZZ_SEED: formatSeed(seed)})).toBe(seed);
  });
});

describe('fuzzBudget', () => {
  test('defaults to 1 and reads a positive integer', () => {
    expect(fuzzBudget({})).toBe(1);
    expect(fuzzBudget({ZERO_FUZZ_BUDGET: '4'})).toBe(4);
  });

  test('rejects zero, fractions and garbage', () => {
    for (const bad of ['0', '2.5', 'lots']) {
      expect(() => fuzzBudget({ZERO_FUZZ_BUDGET: bad})).toThrow(
        /ZERO_FUZZ_BUDGET/,
      );
    }
  });
});
