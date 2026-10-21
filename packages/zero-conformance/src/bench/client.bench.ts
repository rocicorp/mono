/**
 * The JS side of the Swift client's own benchmarks
 * (`swift/Sources/ZeroBenchmarks`), on the same data as
 * `zero-client/src/client/zero.bench.ts`: users with ten number columns. The
 * Swift numbers are only comparable to these when both run on one machine.
 *
 * Pokes and the round trip have no JS counterpart: the JS client applies
 * pokes on the next frame, so timing them measures the scheduling.
 */
import {expect} from 'vitest';
import {bench, describe} from '../../../shared/src/bench.ts';
import {Zero} from '../../../zero-client/src/client/zero.ts';
import {createBuilder} from '../../../zql/src/query/create-builder.ts';

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
  tables: {user},
  relationships: {},
  enableLegacyMutators: true,
} as const;
const zql = createBuilder(schema);

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => {
    resolve = r;
  });
  return {promise, resolve};
}

async function makeZero(rows: number) {
  const z = new Zero({
    schema,
    cacheURL: null,
    userID: 'bench',
    kvStore: 'mem',
    storageKey: `bench-${rows}-${Math.random()}`,
  });
  await z.mutateBatch(async m => {
    for (let n = 0; n < rows; n++) {
      await m.user.insert({
        a: n,
        b: n,
        c: n,
        d: n,
        e: n,
        f: n,
        g: n,
        h: n,
        i: n,
        j: n,
      });
    }
  });
  return z;
}

async function materialize(z: Awaited<ReturnType<typeof makeZero>>, n: number) {
  const {promise, resolve} = deferred<number>();
  const view = z.materialize(zql.user);
  view.addListener(data => {
    if (data.length === n) {
      resolve(data.length);
    }
  });
  expect(await promise).toBe(n);
  view.destroy();
}

// What 100k rows cost in memory, as the Swift benchmark measures it: the
// change in resident memory.
{
  const n = 100_000;
  const before = process.memoryUsage().rss;
  const z = await makeZero(n);
  const view = z.materialize(zql.user);
  const {promise, resolve} = deferred<void>();
  view.addListener(data => {
    if (data.length === n) {
      resolve();
    }
  });
  await promise;
  const mb = (process.memoryUsage().rss - before) / 1e6;
  // oxlint-disable-next-line no-console
  console.log(
    `memory > ${n} rows x 10 columns (numbers), with a view: ${mb.toFixed(1)} MB`,
  );
  view.destroy();
  await z.close();
}

for (const n of [1_000, 10_000, 100_000]) {
  const z = await makeZero(n);
  describe('hydration', () => {
    bench(`materialize ${n} rows`, async () => {
      await materialize(z, n);
    });
  });
}

{
  const n = 1000;
  const z = await makeZero(n);
  const view = z.materialize(zql.user);
  let value = n;
  describe('mutations', () => {
    bench(`optimistic update, ${n}-row view`, async () => {
      value++;
      await z.mutate.user.update({a: value % n, b: value});
    });
  });
  void view;
}
