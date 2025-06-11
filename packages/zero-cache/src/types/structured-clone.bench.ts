import {bench, describe} from 'vitest';

describe('structuredClone benchmark', () => {
  const numString = '12345678.9039098';
  const num = +numString;

  bench('clone number', () => {
    for (let i = 0; i < 1_000_000; i++) {
      structuredClone(num);
    }
  });

  bench('parse and clone number', () => {
    for (let i = 0; i < 1_000_000; i++) {
      const num = +numString;
      structuredClone(num);
    }
  });

  bench('clone string', () => {
    for (let i = 0; i < 1_000_000; i++) {
      structuredClone(numString);
    }
  });
});
