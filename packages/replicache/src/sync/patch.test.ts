import {LogContext} from '@rocicorp/logger';
import {describe, expect, test} from 'vitest';
import {assert} from '../../../shared/src/asserts.ts';
import type {Enum} from '../../../shared/src/enum.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {TestStore} from '../dag/test-store.ts';
import {ChainBuilder} from '../db/test-helpers.ts';
import {newWriteSnapshotDD31} from '../db/write.ts';
import * as FormatVersion from '../format-version-enum.ts';
import {deepFreeze} from '../frozen-json.ts';
import {
  type PatchOperationInternal,
  assertPatchOperations,
} from '../patch-operation.ts';
import {withWriteNoImplicitCommit} from '../with-transactions.ts';
import {apply, optimizePatch} from './patch.ts';

type FormatVersion = Enum<typeof FormatVersion>;

describe('patch', () => {
  const t = (formatVersion: FormatVersion) => {
    const clientID = 'client-id';
    const store = new TestStore();
    const lc = new LogContext();

    type Case = {
      name: string;
      // defaults to new Map([["key", "value"]]);
      existing?: Map<string, JSONValue> | undefined;
      patch: PatchOperationInternal[];
      expErr?: string | undefined;
      expMap?: Map<string, JSONValue> | undefined;
    };

    const cases: Case[] = [
      {
        name: 'put',
        patch: [{op: 'put', key: 'foo', value: 'bar'}],
        expErr: undefined,
        expMap: new Map([
          ['key', 'value'],
          ['foo', 'bar'],
        ]),
      },
      {
        name: 'del',
        patch: [{op: 'del', key: 'key'}],
        expErr: undefined,
        expMap: new Map(),
      },
      {
        name: 'replace',
        patch: [{op: 'put', key: 'key', value: 'newvalue'}],
        expErr: undefined,
        expMap: new Map([['key', 'newvalue']]),
      },
      {
        name: 'put empty key',
        patch: [{op: 'put', key: '', value: 'empty'}],
        expErr: undefined,
        expMap: new Map([
          ['key', 'value'],
          ['', 'empty'],
        ]),
      },
      {
        name: 'put/replace empty key',
        patch: [
          {op: 'put', key: '', value: 'empty'},
          {op: 'put', key: '', value: 'changed'},
        ],
        expErr: undefined,
        expMap: new Map([
          ['key', 'value'],
          ['', 'changed'],
        ]),
      },
      {
        name: 'put/remove empty key',
        patch: [
          {op: 'put', key: '', value: 'empty'},
          {op: 'del', key: ''},
        ],
        expErr: undefined,
        expMap: new Map([['key', 'value']]),
      },
      {
        name: 'top-level clear',
        patch: [{op: 'clear'}],
        expErr: undefined,
        expMap: new Map(),
      },
      {
        name: 'compound ops',
        patch: [
          {op: 'put', key: 'foo', value: 'bar'},
          {op: 'put', key: 'key', value: 'newvalue'},
          {op: 'put', key: 'baz', value: 'baz'},
        ],
        expErr: undefined,
        expMap: new Map([
          ['foo', 'bar'],
          ['key', 'newvalue'],
          ['baz', 'baz'],
        ]),
      },
      {
        name: 'no escaping 1',
        patch: [{op: 'put', key: '~1', value: 'bar'}],
        expErr: undefined,
        expMap: new Map([
          ['key', 'value'],
          ['~1', 'bar'],
        ]),
      },
      {
        name: 'no escaping 2',
        patch: [{op: 'put', key: '~0', value: 'bar'}],
        expErr: undefined,
        expMap: new Map([
          ['key', 'value'],
          ['~0', 'bar'],
        ]),
      },
      {
        name: 'no escaping 3',
        patch: [{op: 'put', key: '/', value: 'bar'}],
        expErr: undefined,
        expMap: new Map([
          ['key', 'value'],
          ['/', 'bar'],
        ]),
      },
      {
        name: 'update no merge no constrain no existing',
        patch: [{op: 'update', key: 'foo'}],
        expErr: undefined,
        expMap: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {}],
        ]),
      },
      {
        name: 'update no merge with constrain no existing',
        patch: [{op: 'update', key: 'foo', constrain: ['bar']}],
        expErr: undefined,
        expMap: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {}],
        ]),
      },
      {
        name: 'update with merge no constrain no existing',
        patch: [
          {op: 'update', key: 'foo', merge: {bar: 'baz', fuzzy: 'wuzzy'}},
        ],
        expErr: undefined,
        expMap: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bar: 'baz', fuzzy: 'wuzzy'}],
        ]),
      },
      {
        name: 'update with merge with constrain no existing',
        patch: [
          {
            op: 'update',
            key: 'foo',
            merge: {bar: 'baz', fuzzy: 'wuzzy'},
            constrain: ['bar'],
          },
        ],
        expErr: undefined,
        expMap: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bar: 'baz'}],
        ]),
      },
      ////
      {
        name: 'update no merge no constrain with existing',
        existing: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bing: 'bong', bar: 'baz'}],
        ]),
        patch: [{op: 'update', key: 'foo'}],
        expErr: undefined,
        expMap: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bing: 'bong', bar: 'baz'}],
        ]),
      },
      {
        name: 'update no merge with constrain with existing',
        existing: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bing: 'bong', bar: 'baz'}],
        ]),
        patch: [{op: 'update', key: 'foo', constrain: ['bar']}],
        expErr: undefined,
        expMap: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bar: 'baz'}],
        ]),
      },
      {
        name: 'update with merge no constrain with existing',
        existing: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bing: 'bong', bar: 'baz'}],
        ]),
        patch: [
          {op: 'update', key: 'foo', merge: {bar: 'baz2', fuzzy: 'wuzzy'}},
        ],
        expErr: undefined,
        expMap: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bing: 'bong', bar: 'baz2', fuzzy: 'wuzzy'}],
        ]),
      },
      {
        name: 'update with merge with constrain with existing',
        existing: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bing: 'bong', bar: 'baz'}],
        ]),
        patch: [
          {
            op: 'update',
            key: 'foo',
            merge: {bar: 'baz2', fuzzy: 'wuzzy'},
            constrain: ['bar', 'bing'],
          },
        ],
        expErr: undefined,
        expMap: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', {bing: 'bong', bar: 'baz2'}],
        ]),
      },
      {
        name: 'update existing is not an object',
        existing: new Map<string, JSONValue>([
          ['key', 'value'],
          ['foo', 'bar'],
        ]),
        patch: [
          {
            op: 'update',
            key: 'foo',
            merge: {bar: 'baz2', fuzzy: 'wuzzy'},
            constrain: ['bar', 'bing'],
          },
        ],
        expErr: 'Invalid type: string `bar`, expected object',
        expMap: undefined,
      },
      {
        name: 'invalid op',
        patch: [{op: 'BOOM', key: 'key'} as unknown as PatchOperationInternal],
        expErr:
          'unknown patch op `BOOM`, expected one of `put`, `del`, `clear`',
        expMap: undefined,
      },
      {
        name: 'invalid key',
        patch: [
          {
            op: 'put',
            key: 42,
            value: true,
          } as unknown as PatchOperationInternal,
        ],
        expErr: 'Invalid type: number `42`, expected string',
        expMap: undefined,
      },
      {
        name: 'missing value',
        patch: [{op: 'put', key: 'k'} as unknown as PatchOperationInternal],
        // expErr: 'missing field `value`',
        expErr: 'Invalid type: undefined, expected JSON value',
        expMap: undefined,
      },
      {
        name: 'missing key for del',
        patch: [{op: 'del'} as unknown as PatchOperationInternal],
        // expErr: 'missing field `key`',
        expErr: 'Invalid type: undefined, expected string',
        expMap: undefined,
      },
      {
        name: 'make sure we do not apply parts of the patch',
        patch: [
          {op: 'put', key: 'k', value: 42},
          {op: 'del'} as unknown as PatchOperationInternal,
        ],
        // expErr: 'missing field `key`',
        expErr: 'Invalid type: undefined, expected string',
        expMap: new Map([['key', 'value']]),
      },
    ];

    for (const c of cases) {
      test(c.name, async () => {
        const b = new ChainBuilder(store, undefined, formatVersion);
        await b.addGenesis(clientID);
        await withWriteNoImplicitCommit(store, async dagWrite => {
          assert(formatVersion >= FormatVersion.DD31);
          const dbWrite = await newWriteSnapshotDD31(
            b.chain[0].chunk.hash,
            {[clientID]: 1},
            'cookie',
            dagWrite,
            clientID,
            formatVersion,
          );

          for (const [key, value] of c.existing ??
            new Map([['key', 'value']])) {
            await dbWrite.put(lc, key, deepFreeze(value));
          }

          const ops = c.patch;

          let err;
          try {
            assertPatchOperations(ops);
            await apply(lc, dbWrite, ops);
          } catch (e) {
            err = e;
          }
          if (c.expErr === undefined && err !== undefined) {
            throw err;
          }
          if (c.expErr !== undefined) {
            expect(err).toBeInstanceOf(Error);
            expect((err as Error).message).toBe(c.expErr);
          }

          if (c.expMap !== undefined) {
            for (const [k, v] of c.expMap) {
              expect(v).toEqual(await dbWrite.get(k));
            }
            if (c.expMap.size === 0) {
              expect(await dbWrite.isEmpty()).toBe(true);
            }
          }
        });
      });
    }
  };

  describe('dd31', () => t(FormatVersion.Latest));
});

describe('optimizePatch', () => {
  test('empty patch', () => {
    expect(optimizePatch([])).toEqual([]);
  });

  test('single operation', () => {
    const patch: PatchOperationInternal[] = [{op: 'put', key: 'a', value: 1}];
    expect(optimizePatch(patch)).toEqual(patch);
  });

  test('drops operations before clear', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'b', value: 2},
      {op: 'del', key: 'a'},
      {op: 'clear'},
      {op: 'put', key: 'c', value: 3},
    ];
    expect(optimizePatch(patch)).toEqual([
      {op: 'clear'},
      {op: 'put', key: 'c', value: 3},
    ]);
  });

  test('keeps only last put for same key', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'a', value: 2},
      {op: 'put', key: 'a', value: 3},
    ];
    expect(optimizePatch(patch)).toEqual([{op: 'put', key: 'a', value: 3}]);
  });

  test('del replaces put', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'del', key: 'a'},
    ];
    expect(optimizePatch(patch)).toEqual([{op: 'del', key: 'a'}]);
  });

  test('keeps del without prior put', () => {
    const patch: PatchOperationInternal[] = [{op: 'del', key: 'a'}];
    expect(optimizePatch(patch)).toEqual([{op: 'del', key: 'a'}]);
  });

  test('put after del keeps only put', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'del', key: 'a'},
      {op: 'put', key: 'a', value: 1},
    ];
    expect(optimizePatch(patch)).toEqual([{op: 'put', key: 'a', value: 1}]);
  });

  test('preserves update operations', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: {x: 1}},
      {op: 'update', key: 'a', merge: {y: 2}},
      {op: 'put', key: 'b', value: 3},
    ];
    // put + update should be merged into single put
    expect(optimizePatch(patch)).toEqual([
      {op: 'put', key: 'a', value: {x: 1, y: 2}},
      {op: 'put', key: 'b', value: 3},
    ]);
  });

  test('multiple clears collapse to last', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'clear'},
      {op: 'put', key: 'b', value: 2},
      {op: 'clear'},
      {op: 'put', key: 'c', value: 3},
    ];
    expect(optimizePatch(patch)).toEqual([
      {op: 'clear'},
      {op: 'put', key: 'c', value: 3},
    ]);
  });

  test('complex optimization', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'b', value: 2},
      {op: 'del', key: 'a'}, // del replaces put
      {op: 'clear'}, // Everything above gets dropped
      {op: 'put', key: 'c', value: 3},
      {op: 'put', key: 'd', value: 4},
      {op: 'put', key: 'c', value: 5}, // Overwrites c=3
      {op: 'del', key: 'e'}, // Del after clear without prior put, removed
      {op: 'put', key: 'f', value: 6},
      {op: 'del', key: 'f'}, // del replaces put
    ];
    const result = optimizePatch(patch);
    expect(result).toEqual([
      {op: 'clear'},
      {key: 'c', op: 'put', value: 5},
      {key: 'd', op: 'put', value: 4},
    ]);
  });

  test('maintains relative order of operations', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'b', value: 2},
      {op: 'put', key: 'c', value: 3},
    ];
    const optimized = optimizePatch(patch);
    // Order should be preserved
    expect(optimized).toEqual(patch);
  });

  test('update operations orders position', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'b', value: {b: 1}},
      {op: 'put', key: 'a', value: {a: 2}},
      {op: 'update', key: 'a', merge: {a2: 3}},
      {op: 'update', key: 'b', merge: {b2: 4}},
    ];
    // put + update should be merged for 'a' (object value)
    // but 'b' keeps separate update (non-object value)
    const optimized = optimizePatch(patch);
    expect(optimized).toEqual([
      {op: 'put', key: 'a', value: {a: 2, a2: 3}},
      {op: 'put', key: 'b', value: {b: 1, b2: 4}},
    ]);
  });

  test('clear in middle with operations on both sides', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'b', value: 2},
      {op: 'clear'},
      {op: 'put', key: 'c', value: 3},
      {op: 'put', key: 'd', value: 4},
    ];
    expect(optimizePatch(patch)).toEqual([
      {op: 'clear'},
      {op: 'put', key: 'c', value: 3},
      {op: 'put', key: 'd', value: 4},
    ]);
  });

  test('multiple updates accumulate', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: {b: 1, c: 2}},
      {op: 'update', key: 'a', merge: {c: 22}},
      {op: 'update', key: 'a', merge: {b: 11}},
    ];
    const result = optimizePatch(patch);
    // put + updates should be merged into single put with accumulated updates
    expect(result).toEqual([{op: 'put', key: 'a', value: {b: 11, c: 22}}]);
  });

  test('put replaces previous updates', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: {b: 1}},
      {op: 'update', key: 'a', merge: {c: 2}},
      {op: 'update', key: 'a', merge: {d: 3}},
      {op: 'put', key: 'a', value: {e: 4}},
    ];
    expect(optimizePatch(patch)).toEqual([
      {op: 'put', key: 'a', value: {e: 4}},
    ]);
  });

  test('del replaces previous updates', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: {b: 1}},
      {op: 'update', key: 'a', merge: {c: 2}},
      {op: 'update', key: 'a', merge: {d: 3}},
      {op: 'del', key: 'a'},
    ];
    expect(optimizePatch(patch)).toEqual([{op: 'del', key: 'a'}]);
  });

  test('updates without initial put', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'update', key: 'a', merge: {b: 1}},
      {op: 'update', key: 'a', merge: {c: 2}},
    ];
    expect(optimizePatch(patch)).toEqual(patch);
  });

  test('removes del after clear without put', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'clear'},
      {op: 'del', key: 'a'},
      {op: 'put', key: 'b', value: 1},
      {op: 'del', key: 'b'},
      {op: 'put', key: 'c', value: 2},
    ];
    expect(optimizePatch(patch)).toEqual([
      {op: 'clear'},
      {op: 'put', key: 'c', value: 2},
    ]);
  });

  test('keys are sorted alphabetically in optimized patch', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'z', value: 26},
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'm', value: 13},
      {op: 'put', key: 'b', value: 2},
    ];
    const result = optimizePatch(patch);
    expect(result).toEqual([
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'b', value: 2},
      {op: 'put', key: 'm', value: 13},
      {op: 'put', key: 'z', value: 26},
    ]);
  });

  test('clear with no following operations', () => {
    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'b', value: 2},
      {op: 'clear'},
    ];
    expect(optimizePatch(patch)).toEqual([{op: 'clear'}]);
  });
});

describe('patch with optimization', () => {
  const formatVersion = FormatVersion.Latest;
  const clientID = 'client-id';
  const lc = new LogContext();

  test('optimized patch produces same result as non-optimized', async () => {
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);

    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'a', value: 2}, // Duplicate
      {op: 'put', key: 'b', value: 3},
      {op: 'put', key: 'c', value: 4},
      {op: 'del', key: 'c'}, // Put+del pair
    ];

    // Apply with optimization (automatic in apply function)
    await withWriteNoImplicitCommit(store, async dagWrite => {
      const dbWrite = await newWriteSnapshotDD31(
        b.chain[0].chunk.hash,
        {[clientID]: 1},
        'cookie',
        dagWrite,
        clientID,
        formatVersion,
      );
      await apply(lc, dbWrite, patch);

      expect(await dbWrite.get('a')).toBe(2);
      expect(await dbWrite.get('b')).toBe(3);
      expect(await dbWrite.get('c')).toBeUndefined();
    });
  });

  test('bulk load after clear', async () => {
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);

    const patch: PatchOperationInternal[] = [
      {op: 'clear'},
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'b', value: 2},
      {op: 'put', key: 'c', value: 3},
      {op: 'put', key: 'd', value: 4},
      {op: 'put', key: 'e', value: 5},
    ];

    await withWriteNoImplicitCommit(store, async dagWrite => {
      const dbWrite = await newWriteSnapshotDD31(
        b.chain[0].chunk.hash,
        {[clientID]: 1},
        'cookie',
        dagWrite,
        clientID,
        formatVersion,
      );
      await apply(lc, dbWrite, patch);

      expect(await dbWrite.get('a')).toBe(1);
      expect(await dbWrite.get('b')).toBe(2);
      expect(await dbWrite.get('c')).toBe(3);
      expect(await dbWrite.get('d')).toBe(4);
      expect(await dbWrite.get('e')).toBe(5);
    });
  });

  test('bulk load at start (no clear)', async () => {
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);

    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'b', value: 2},
      {op: 'put', key: 'c', value: 3},
    ];

    await withWriteNoImplicitCommit(store, async dagWrite => {
      const dbWrite = await newWriteSnapshotDD31(
        b.chain[0].chunk.hash,
        {[clientID]: 1},
        'cookie',
        dagWrite,
        clientID,
        formatVersion,
      );
      await apply(lc, dbWrite, patch);

      expect(await dbWrite.get('a')).toBe(1);
      expect(await dbWrite.get('b')).toBe(2);
      expect(await dbWrite.get('c')).toBe(3);
    });
  });

  test('mixed operations with update', async () => {
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);

    const patch: PatchOperationInternal[] = [
      {op: 'put', key: 'a', value: {x: 1}},
      {op: 'put', key: 'b', value: {y: 2}},
      {op: 'update', key: 'a', merge: {z: 3}},
      {op: 'put', key: 'c', value: 4},
    ];

    await withWriteNoImplicitCommit(store, async dagWrite => {
      const dbWrite = await newWriteSnapshotDD31(
        b.chain[0].chunk.hash,
        {[clientID]: 1},
        'cookie',
        dagWrite,
        clientID,
        formatVersion,
      );
      await apply(lc, dbWrite, patch);

      expect(await dbWrite.get('a')).toEqual({x: 1, z: 3});
      expect(await dbWrite.get('b')).toEqual({y: 2});
      expect(await dbWrite.get('c')).toBe(4);
    });
  });

  test('unsorted keys get sorted during bulk load', async () => {
    const store = new TestStore();
    const b = new ChainBuilder(store, undefined, formatVersion);
    await b.addGenesis(clientID);

    const patch: PatchOperationInternal[] = [
      {op: 'clear'},
      {op: 'put', key: 'z', value: 26},
      {op: 'put', key: 'a', value: 1},
      {op: 'put', key: 'm', value: 13},
      {op: 'put', key: 'b', value: 2},
    ];

    await withWriteNoImplicitCommit(store, async dagWrite => {
      const dbWrite = await newWriteSnapshotDD31(
        b.chain[0].chunk.hash,
        {[clientID]: 1},
        'cookie',
        dagWrite,
        clientID,
        formatVersion,
      );
      await apply(lc, dbWrite, patch);

      expect(await dbWrite.get('a')).toBe(1);
      expect(await dbWrite.get('b')).toBe(2);
      expect(await dbWrite.get('m')).toBe(13);
      expect(await dbWrite.get('z')).toBe(26);
    });
  });
});
