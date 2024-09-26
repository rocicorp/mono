import {expect} from 'chai';
import type {ReadonlyJSONValue} from 'shared/src/json.js';
import {asyncIterableToArray} from './async-iterable-to-array.js';
import type {IndexKey} from './db/index.js';
import {
  type Entry,
  type GetIndexScanIterator,
  type GetScanIterator,
  fromKeyForIndexScan,
  makeScanResult,
} from './scan-iterator.js';
import type {ScanIndexOptions, ScanOptions} from './scan-options.js';

test('makeScanResult', async () => {
  function getTestScanAsyncIterator(
    entries: (readonly [key: string, value: ReadonlyJSONValue])[],
  ): GetScanIterator {
    return async function* (fromKey: string) {
      for (const [key, value] of entries) {
        if (key >= fromKey) {
          yield [key, value] as const;
        }
      }
    };
  }

  function getTestScanIterator(
    entries: (readonly [key: string, value: ReadonlyJSONValue])[],
  ): GetScanIterator {
    return function* (fromKey: string) {
      for (const [key, value] of entries) {
        if (key >= fromKey) {
          yield [key, value] as const;
        }
      }
    };
  }

  const t = async (
    entries: Entry<ReadonlyJSONValue>[],
    options: ScanOptions,
    expectedEntries = entries,
  ) => {
    {
      const iter = makeScanResult(options, getTestScanAsyncIterator(entries));
      expect(await asyncIterableToArray(iter.entries())).to.deep.equal(
        expectedEntries,
      );
    }
    {
      const iter = makeScanResult(options, getTestScanIterator(entries));
      expect(await asyncIterableToArray(iter.entries())).to.deep.equal(
        expectedEntries,
      );
    }
  };

  await t([], {});
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {},
  );

  // prefix
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {prefix: 'a'},
    [],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {prefix: 'b'},
    [['b', 1]],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {prefix: 'c'},
    [],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {prefix: 'd'},
    [['d', 2]],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {prefix: 'e'},
    [],
  );

  // start
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'a'}},
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'b'}},
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'c'}},
    [['d', 2]],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'd'}},
    [['d', 2]],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'e'}},
    [],
  );

  // start exclusive
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'a', exclusive: true}},
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'b', exclusive: true}},
    [['d', 2]],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'c', exclusive: true}},
    [['d', 2]],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'd', exclusive: true}},
    [],
  );
  await t(
    [
      ['b', 1],
      ['d', 2],
    ],
    {start: {key: 'e', exclusive: true}},
    [],
  );

  // start & prefix
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'a', start: {key: 'aa'}},
    [
      ['ab', 1],
      ['ad', 2],
    ],
  );
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'a', start: {key: 'ab'}},
    [
      ['ab', 1],
      ['ad', 2],
    ],
  );
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'a', start: {key: 'ac'}},
    [['ad', 2]],
  );
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'a', start: {key: 'ad'}},
    [['ad', 2]],
  );
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'a', start: {key: 'ae'}},
    [],
  );

  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'b', start: {key: 'aa'}},
    [
      ['bf', 3],
      ['bh', 4],
    ],
  );
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'b', start: {key: 'ab'}},
    [
      ['bf', 3],
      ['bh', 4],
    ],
  );
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'b', start: {key: 'ad'}},
    [
      ['bf', 3],
      ['bh', 4],
    ],
  );
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'b', start: {key: 'bf'}},
    [
      ['bf', 3],
      ['bh', 4],
    ],
  );
  await t(
    [
      ['ab', 1],
      ['ad', 2],
      ['bf', 3],
      ['bh', 4],
    ],
    {prefix: 'b', start: {key: 'bh'}},
    [['bh', 4]],
  );
});

test('makeScanResult with index', async () => {
  function getTestScanAsyncIterator(
    entries: (readonly [key: IndexKey, value: ReadonlyJSONValue])[],
  ): GetIndexScanIterator {
    return async function* (indexName, secondaryKey, primaryKey) {
      expect(indexName).to.equal('index');
      for (const [key, value] of entries) {
        if (key[0] >= secondaryKey) {
          if (primaryKey === undefined || key[1] >= primaryKey) {
            yield [key, value] as const;
          }
        }
      }
    };
  }

  function getTestScanIterator(
    entries: (readonly [key: IndexKey, value: ReadonlyJSONValue])[],
  ): GetIndexScanIterator {
    return function* (indexName, secondaryKey, primaryKey) {
      expect(indexName).to.equal('index');
      for (const [key, value] of entries) {
        if (key[0] >= secondaryKey) {
          if (primaryKey === undefined || key[1] >= primaryKey) {
            yield [key, value] as const;
          }
        }
      }
    };
  }

  const t = async (
    entries: (readonly [key: IndexKey, value: ReadonlyJSONValue])[],
    options: Omit<ScanIndexOptions, 'indexName'> = {},
    expectedEntries = entries,
  ) => {
    {
      const indexOptions = {indexName: 'index', ...options};
      const iter = makeScanResult(
        indexOptions,
        getTestScanAsyncIterator(entries),
      );
      expect(await asyncIterableToArray(iter.entries())).to.deep.equal(
        expectedEntries,
      );
    }
    {
      const indexOptions = {indexName: 'index', ...options};
      const iter = makeScanResult(indexOptions, getTestScanIterator(entries));
      expect(await asyncIterableToArray(iter.entries())).to.deep.equal(
        expectedEntries,
      );
    }
  };

  await t([]);
  await t([[['sb', 'pb'], 1]]);
  await t([
    [['sb', 'pb'], 1],
    [['sd', 'pd'], 2],
  ]);

  // prefix is always on secondary
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 'sa'},
    [],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 'sb'},
    [[['sb', 'pb'], 1]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 'sc'},
    [],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 'sd'},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 's'},
  );

  // start key
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: 'sa'}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sa']}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sa', 'pa']}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: 'sb'}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sb']}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sb', '']}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sb', 'pb']}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sb', 'pc']}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: 'sc'}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: 'sd'}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sd', 'pc']}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sd', 'pd']}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sd', 'pe']}},
    [],
  );

  // start exclusive
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: 'sa', exclusive: true}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sa'], exclusive: true}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sa', 'pa'], exclusive: true}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: 'sb', exclusive: true}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sb'], exclusive: true}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sb', ''], exclusive: true}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sb', 'pb'], exclusive: true}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sb', 'pc'], exclusive: true}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: 'sc', exclusive: true}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: 'sd', exclusive: true}},
    [],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sd', 'pc'], exclusive: true}},
    [[['sd', 'pd'], 2]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sd', 'pd'], exclusive: true}},
    [],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {start: {key: ['sd', 'pe'], exclusive: true}},
    [],
  );

  // prefix and start
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 'sa', start: {key: 'sb'}},
    [],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 's', start: {key: 'sb'}},
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 'sb', start: {key: 'sb'}},
    [[['sb', 'pb'], 1]],
  );
  await t(
    [
      [['sb', 'pb'], 1],
      [['sd', 'pd'], 2],
    ],
    {prefix: 'sd', start: {key: 'sb'}},
    [[['sd', 'pd'], 2]],
  );
});

test('fromKeyForIndexScan', () => {
  const t = (
    options: Omit<ScanIndexOptions, 'indexName'>,
    expected: [secondary: string, primary?: string | undefined],
  ) => {
    const indexOptions = {indexName: 'i', ...options};
    expect(fromKeyForIndexScan(indexOptions)).to.deep.equal(expected);
  };

  t({}, ['', undefined]);
  t({prefix: 'a'}, ['a', undefined]);
  t({start: {key: 'a'}}, ['a']);
  t({start: {key: ['a']}}, ['a']);
  t({start: {key: ['a', undefined]}}, ['a', undefined]);
  t({start: {key: ['a', 'b']}}, ['a', 'b']);

  t({prefix: 'b', start: {key: 'a'}}, ['b', undefined]);
  t({prefix: 'a', start: {key: 'b'}}, ['b']);
  t({prefix: 'a', start: {key: ['a']}}, ['a', undefined]);
  t({prefix: 'a', start: {key: ['a', '']}}, ['a', '']);
});
