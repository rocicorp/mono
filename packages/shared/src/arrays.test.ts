import {describe, expect, test} from 'vitest';
import {defined} from './arrays.js';

describe('shared/arrays', () => {
  type Case = {
    input: (number | undefined)[];
    output: number[];
  };

  const cases: Case[] = [
    {
      input: [],
      output: [],
    },
    {
      input: [undefined],
      output: [],
    },
    {
      input: [undefined, undefined],
      output: [],
    },
    {
      input: [0, undefined],
      output: [0],
    },
    {
      input: [undefined, 0],
      output: [0],
    },
    {
      input: [undefined, 0, undefined],
      output: [0],
    },
    {
      input: [undefined, 0, 1],
      output: [0, 1],
    },
    {
      input: [0, undefined, 1],
      output: [0, 1],
    },
    {
      input: [0, undefined, 0, 1],
      output: [0, 0, 1],
    },
    {
      input: [0, undefined, 0, 1, undefined],
      output: [0, 0, 1],
    },
    {
      input: [0, undefined, 0, undefined, 1, undefined],
      output: [0, 0, 1],
    },
    {
      input: [2, 1, 0, undefined, 0, undefined, 1, undefined, 2],
      output: [2, 1, 0, 0, 1, 2],
    },
  ];

  for (const c of cases) {
    test(`defined(${JSON.stringify(c.input)})`, () => {
      const output = defined(c.input);
      expect(output).toEqual(c.output);
      if (output.length === c.input.length) {
        expect(output).toBe(c.input); // No copy
      }
    });
  }
});
