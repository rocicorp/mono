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
import {apply} from './patch.ts';

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
            expect(err).to.be.instanceOf(Error);
            expect((err as Error).message).to.equal(c.expErr);
          }

          if (c.expMap !== undefined) {
            for (const [k, v] of c.expMap) {
              expect(v).to.deep.equal(await dbWrite.get(k));
            }
            if (c.expMap.size === 0) {
              expect(await dbWrite.isEmpty()).to.be.true;
            }
          }
        });
      });
    }
  };

  describe('dd31', () => t(FormatVersion.Latest));
});
