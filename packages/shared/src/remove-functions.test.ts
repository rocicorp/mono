import {describe, expect, test} from 'vitest';
import {removeFunctions} from './remove-functions.ts';

describe('removeFunctions', () => {
  test('handles primitives', () => {
    expect(removeFunctions(42)).toBe(42);
    expect(removeFunctions('hello')).toBe('hello');
    expect(removeFunctions(true)).toBe(true);
    expect(removeFunctions(null)).toBe(null);
    expect(removeFunctions(undefined)).toBe(undefined);
  });

  test('removes function properties from object', () => {
    const obj = {
      name: 'foo',
      compute: () => 42,
      value: 123,
    };

    const result = removeFunctions(obj);

    expect(result).toEqual({name: 'foo', value: 123});
    expect(result).not.toHaveProperty('compute');
  });

  test('handles nested objects with functions', () => {
    const obj = {
      outer: {
        inner: {
          data: 'value',
          fn: () => 'hidden',
        },
        compute: () => 123,
      },
      value: 42,
    };

    const result = removeFunctions(obj);

    expect(result).toEqual({
      outer: {
        inner: {
          data: 'value',
        },
      },
      value: 42,
    });
  });

  test('handles arrays', () => {
    const obj = {
      items: [
        {id: 1, fn: () => 'a'},
        {id: 2, fn: () => 'b'},
      ],
    };

    const result = removeFunctions(obj);

    expect(result).toEqual({
      items: [{id: 1}, {id: 2}],
    });
  });

  test('handles nested arrays', () => {
    const obj = {
      matrix: [
        [
          {x: 1, fn: () => 'a'},
          {x: 2, fn: () => 'b'},
        ],
        [{x: 3, fn: () => 'c'}],
      ],
    };

    const result = removeFunctions(obj);

    expect(result).toEqual({
      matrix: [[{x: 1}, {x: 2}], [{x: 3}]],
    });
  });

  test('removes Map instances', () => {
    const obj = {
      data: 'value',
      map: new Map([
        ['key1', 'value1'],
        ['key2', 'value2'],
      ]),
    };

    const result = removeFunctions(obj);

    expect(result).toEqual({data: 'value'});
    expect(result).not.toHaveProperty('map');
  });

  test('removes Set instances', () => {
    const obj = {
      data: 'value',
      set: new Set([1, 2, 3]),
    };

    const result = removeFunctions(obj);

    expect(result).toEqual({data: 'value'});
    expect(result).not.toHaveProperty('set');
  });

  test('preserves undefined values', () => {
    const obj = {
      a: 1,
      b: undefined,
      c: null,
    };

    const result = removeFunctions(obj);

    expect(result).toEqual({a: 1, b: undefined, c: null});
    expect(result).toHaveProperty('b');
  });

  test('handles circular references', () => {
    const obj: Record<string, unknown> = {
      name: 'circular',
    };
    obj.self = obj;

    const result = removeFunctions(obj);

    expect(result).toHaveProperty('name', 'circular');
    expect(result).toHaveProperty('self', undefined);
  });

  test('handles complex nested structures', () => {
    const obj = {
      type: 'event',
      costs: [
        {
          connection: 'users',
          cost: 10,
          costEstimate: {
            startupCost: 0,
            scanEst: 10,
            cost: 10,
            returnedRows: 100,
            selectivity: 1.0,
            limit: undefined,
            fanout: () => ({fanout: 1, confidence: 'high'}),
          },
          pinned: false,
          constraints: {id: {id: undefined}},
          constraintCosts: {
            id: {
              startupCost: 0,
              scanEst: 5,
              cost: 5,
              returnedRows: 10,
              selectivity: 0.1,
              limit: undefined,
              fanout: () => ({fanout: 1, confidence: 'high'}),
            },
          },
        },
      ],
    };

    const result = removeFunctions(obj);

    expect(result).toEqual({
      type: 'event',
      costs: [
        {
          connection: 'users',
          cost: 10,
          costEstimate: {
            startupCost: 0,
            scanEst: 10,
            cost: 10,
            returnedRows: 100,
            selectivity: 1.0,
            limit: undefined,
          },
          pinned: false,
          constraints: {id: {id: undefined}},
          constraintCosts: {
            id: {
              startupCost: 0,
              scanEst: 5,
              cost: 5,
              returnedRows: 10,
              selectivity: 0.1,
              limit: undefined,
            },
          },
        },
      ],
    });

    // Verify fanout is removed
    expect(
      (result as {costs: Array<{costEstimate: unknown}>}).costs[0].costEstimate,
    ).not.toHaveProperty('fanout');
  });

  test('does not modify original object', () => {
    const obj = {
      name: 'test',
      fn: () => 42,
      nested: {
        value: 1,
        compute: () => 'hidden',
      },
    };

    const original = {...obj};
    removeFunctions(obj);

    // Original should be unchanged
    expect(obj).toEqual(original);
    expect(obj).toHaveProperty('fn');
    expect(obj.nested).toHaveProperty('compute');
  });
});
