import {describe, expect, test} from 'vitest';
import {defined, groupBy, zip} from './arrays.ts';

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

describe('zip', () => {
  test('zips empty arrays', () => {
    expect(zip([], [])).toEqual([]);
  });

  test('zips arrays of equal length', () => {
    expect(zip([1, 2, 3], ['a', 'b', 'c'])).toEqual([
      [1, 'a'],
      [2, 'b'],
      [3, 'c'],
    ]);
  });

  test('zips arrays with same elements', () => {
    expect(zip([1, 1, 1], [2, 2, 2])).toEqual([
      [1, 2],
      [1, 2],
      [1, 2],
    ]);
  });

  test('throws on arrays of different length', () => {
    expect(() => zip([1, 2], [1])).toThrow();
    expect(() => zip([1], [1, 2])).toThrow();
  });

  test('preserves element references', () => {
    const obj1 = {id: 1};
    const obj2 = {id: 2};
    const arr1 = [obj1];
    const arr2 = [obj2];
    const result = zip(arr1, arr2);
    expect(result[0][0]).toBe(obj1);
    expect(result[0][1]).toBe(obj2);
  });
});

describe('groupBy', () => {
  test('groups empty array', () => {
    expect(groupBy([], x => x)).toEqual(new Map());
  });

  test('groups numbers by value', () => {
    const input = [1, 2, 1, 3, 2, 1];
    const result = groupBy(input, x => x);
    expect(result.size).toBe(3);
    expect(result.get(1)).toEqual([1, 1, 1]);
    expect(result.get(2)).toEqual([2, 2]);
    expect(result.get(3)).toEqual([3]);
  });

  test('groups objects by property', () => {
    const input = [
      {category: 'A', value: 1},
      {category: 'B', value: 2},
      {category: 'A', value: 3},
      {category: 'C', value: 4},
      {category: 'B', value: 5},
    ];
    const result = groupBy(input, x => x.category);

    expect(result.size).toBe(3);
    expect(result.get('A')).toEqual([
      {category: 'A', value: 1},
      {category: 'A', value: 3},
    ]);
    expect(result.get('B')).toEqual([
      {category: 'B', value: 2},
      {category: 'B', value: 5},
    ]);
    expect(result.get('C')).toEqual([{category: 'C', value: 4}]);
  });

  test('preserves element references', () => {
    const obj1 = {id: 1, group: 'A'};
    const obj2 = {id: 2, group: 'A'};
    const input = [obj1, obj2];
    const result = groupBy(input, x => x.group);
    const groupA = result.get('A')!;
    expect(groupA[0]).toBe(obj1);
    expect(groupA[1]).toBe(obj2);
  });

  test('groups by computed values', () => {
    const input = [1, 2, 3, 4, 5, 6];
    const result = groupBy(input, x => (x % 2 === 0 ? 'even' : 'odd'));
    expect(result.get('even')).toEqual([2, 4, 6]);
    expect(result.get('odd')).toEqual([1, 3, 5]);
  });

  test('handles different key types', () => {
    const input = ['a', 'bb', 'ccc', 'd', 'ee'];
    const result = groupBy(input, x => x.length);
    expect(result.get(1)).toEqual(['a', 'd']);
    expect(result.get(2)).toEqual(['bb', 'ee']);
    expect(result.get(3)).toEqual(['ccc']);
  });
});
