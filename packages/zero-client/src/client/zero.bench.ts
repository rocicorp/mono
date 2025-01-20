import {resolver} from '@rocicorp/resolver';
import {bench, describe, expect} from 'vitest';
import {sleep} from '../../../shared/src/sleep.js';
import type {Row} from '../../../zql/src/query/query.js';
import {getInternalReplicacheImplForTesting, Zero} from './zero.js';

const user = {
  name: 'user',
  columns: {
    a: {type: 'number'},
    b: {type: 'number'},
    c: {type: 'number'},
    d: {type: 'number'},
    e: {type: 'number'},
    f: {type: 'number'},
    g: {type: 'number'},
    h: {type: 'number'},
    i: {type: 'number'},
    j: {type: 'number'},
  },
  primaryKey: ['a'],
} as const;
const schema = {
  version: 0,
  tables: {
    user,
  },
  relationships: {},
} as const;
type Schema = typeof schema;
type UserRow = Row<typeof user>;

const userID = 'test-user-id-' + Math.random();

async function withZero(
  f: (z: Zero<Schema>) => Promise<void>,
  persist = false,
): Promise<void> {
  const z = new Zero({
    schema,
    server: null,
    userID,
    kvStore: 'idb',
  });
  await f(z);
  if (persist) {
    await sleep(500);
    await getInternalReplicacheImplForTesting(z).persist();
  }
  await z.close();
}
const N = 1_000;

describe('basics', () => {
  bench(
    `All ${N} rows x 10 columns (numbers)`,
    () =>
      withZero(async z => {
        const {promise, resolve} = resolver<readonly UserRow[]>();
        const m = z.query.user.materialize();
        m.addListener(data => {
          if (data.length === N) {
            resolve(data as readonly UserRow[]);
          }
        });
        const rows = await promise;
        expect(rows.reduce((sum, row) => sum + row.a, 0)).toBe(
          ((N - 1) / 2) * N,
        );
        m.destroy();
      }),
    {
      throws: true,
      setup: () =>
        withZero(async z => {
          await z.mutateBatch(async m => {
            for (let i = 0; i < N; i++) {
              await m.user.insert({
                a: i,
                b: i,
                c: i,
                d: i,
                e: i,
                f: i,
                g: i,
                h: i,
                i,
                j: i,
              });
            }
          });
        }, true),

      teardown: () =>
        withZero(async z => {
          await z.mutateBatch(async m => {
            for (let i = 0; i < N; i++) {
              await m.user.delete({a: i});
            }
          });
        }, true),
    },
  );
});

describe('with filter', () => {
  bench(
    `Lower rows ${N / 2} x 10 columns (numbers)`,
    () =>
      withZero(async z => {
        const {promise, resolve} = resolver<readonly UserRow[]>();
        const m = z.query.user.where('a', '<', N / 2).materialize();
        m.addListener(data => {
          if (data.length === N / 2) {
            resolve(data as readonly UserRow[]);
          }
        });
        const rows = await promise;
        expect(rows.reduce((sum, row) => sum + row.a, 0)).toBe(
          (((N / 2 - 1) / 2) * N) / 2,
        );
        m.destroy();
      }),
    {
      throws: true,
      setup: () =>
        withZero(async z => {
          await z.mutateBatch(async m => {
            for (let i = 0; i < N; i++) {
              await m.user.insert({
                a: i,
                b: i,
                c: i,
                d: i,
                e: i,
                f: i,
                g: i,
                h: i,
                i,
                j: i,
              });
            }
          });
        }, true),

      teardown: () =>
        withZero(async z => {
          await z.mutateBatch(async m => {
            for (let i = 0; i < N; i++) {
              await m.user.delete({a: i});
            }
          });
        }, true),
    },
  );
});
