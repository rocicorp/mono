import {compareUTF8} from 'compare-utf8';
import {describe, expect, test} from 'vitest';
import {parse, stringify} from './bigint-json.js';
import {
  InvalidationFilterSpec,
  InvalidationTag,
  invalidationHash,
  normalizeFilterSpec,
  parseFilterSpec,
} from './invalidation.js';

describe('types/invalidation', () => {
  type HashCase = {
    name: string;
    tag: InvalidationTag;
    hash: string;
  };

  const hashCases: HashCase[] = [
    {
      name: 'TableTag',
      tag: {schema: 'public', table: 'foo'},
      hash: '9157efde7f0c5890',
    },
    {
      name: 'RowTag with no filteredColumns should be equivalent to TableTag',
      tag: {schema: 'public', table: 'foo', filteredColumns: {}},
      hash: '9157efde7f0c5890', // Same as TableTag
    },
    {
      name: 'RowTag with selectedColumns should not be equivalent to TableTag',
      tag: {
        schema: 'public',
        table: 'foo',
        filteredColumns: {},
        selectedColumns: ['bar'],
      },
      hash: '0438451ab415ecac', // Different from TableTag
    },
    {
      name: 'RowTag with selectedColumns but no filtered columns',
      tag: {
        schema: 'public',
        table: 'foo',
        selectedColumns: ['bar'],
      },
      hash: '0438451ab415ecac', // Same as preceding
    },
    {
      name: 'FullTableTag',
      tag: {schema: 'public', table: 'foo', allRows: true},
      hash: '2b2fae818d8427ad', // Different from TableTag.
    },
    {
      name: 'RowTag should not be confused with FullTableTag',
      tag: {schema: 'public', table: 'foo', filteredColumns: {allRows: 'true'}},
      hash: 'c75cf88cadbbe05e', // Different from FullTableTag
    },
    {
      name: 'RowTag with multiple filtered and selected columns',
      tag: {
        schema: 'public',
        table: 'foo',
        filteredColumns: {
          foo: 'bar',
          baz: 'bonk',
        },
        selectedColumns: ['bar', 'baz', 'foo'],
      },
      hash: '8c447dd98dcea0fb',
    },
    {
      name: 'RowTag with multiple filtered and selected columns (different order)',
      tag: {
        schema: 'public',
        table: 'foo',
        filteredColumns: {
          baz: 'bonk',
          foo: 'bar',
        },
        selectedColumns: ['foo', 'baz', 'bar'],
      },
      hash: '8c447dd98dcea0fb', // Same as preceding
    },
  ];

  for (const c of hashCases) {
    test(`invalidationHash: ${c.name}`, () => {
      expect(invalidationHash(c.tag)).toBe(c.hash);
      // Verify back/forth serialization (requires whole-byte padding of hex string).
      expect(Buffer.from(c.hash, 'hex').toString('hex')).toBe(c.hash);
    });
  }

  type SpecCase = {
    name: string;
    specs: InvalidationFilterSpec[];
    json: string;
  };

  const specCases: SpecCase[] = [
    {
      name: 'filteredColumn ordering',
      specs: [
        {
          schema: 'public',
          table: 'foo',
          filteredColumns: {
            a: '=',
            c: '=',
          },
        },
        {
          schema: 'public',
          table: 'foo',
          filteredColumns: {
            c: '=',
            a: '=',
          },
        },
        {
          filteredColumns: {
            c: '=',
            a: '=',
          },
          schema: 'public',
          table: 'foo',
        },
        {
          schema: 'public',
          filteredColumns: {
            c: '=',
            a: '=',
          },
          table: 'foo',
        },
      ],
      json: '{"id":"33sa2qkxzbodj","schema":"public","table":"foo","filteredColumns":{"a":"=","c":"="}}',
    },
    {
      name: 'selectedColumn ordering',
      specs: [
        {
          schema: 'public',
          table: 'foo',
          filteredColumns: {
            a: '=',
            c: '=',
          },
          selectedColumns: ['z', 'x', 'y'],
        },
        {
          schema: 'public',
          table: 'foo',
          filteredColumns: {
            c: '=',
            a: '=',
          },
          selectedColumns: ['x', 'y', 'z'],
        },
        {
          filteredColumns: {
            c: '=',
            a: '=',
          },
          selectedColumns: ['y', 'z', 'x'],
          schema: 'public',
          table: 'foo',
        },
        {
          selectedColumns: ['z', 'y', 'x'],
          schema: 'public',
          filteredColumns: {
            c: '=',
            a: '=',
          },
          table: 'foo',
        },
      ],
      json: '{"id":"69avsc9c5tkf","schema":"public","table":"foo","filteredColumns":{"a":"=","c":"="},"selectedColumns":["x","y","z"]}',
    },
  ];

  for (const c of specCases) {
    test(`filter spec normalization: ${c.name}`, () => {
      for (const spec of c.specs) {
        const normalized = stringify(normalizeFilterSpec(spec));

        expect(normalized).toBe(c.json);

        const reparsed = parseFilterSpec(parse(normalized));
        const filteredCols = Object.keys(reparsed.filteredColumns);
        expect(filteredCols).toEqual(
          Object.keys(spec.filteredColumns).sort(compareUTF8),
        );

        if (spec.selectedColumns) {
          expect(reparsed.selectedColumns).toEqual(
            [...spec.selectedColumns].sort(compareUTF8),
          );
        }
      }
    });
  }
});
