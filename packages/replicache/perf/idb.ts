import {openDB, deleteDB, IDBPDatabase} from 'idb/with-async-ittr';
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore Invalid module when using node16 module resolution.
import xbytes from 'xbytes';
import {randomData, RandomDataType} from './data.js';
import type {Bencher, Benchmark} from './perf.js';

export function benchmarkIDBRead(opts: {
  dataType: RandomDataType;
  group: string;
  valSize: number;
  numKeys: number;
}): Benchmark {
  return {
    name: `idb read tx (${opts.dataType}) ${opts.numKeys}x${xbytes(
      opts.valSize,
      {
        fixed: 0,
        iec: true,
      },
    )}`,
    group: opts.group,
    byteSize: opts.valSize * opts.numKeys,
    async setup() {
      await deleteDB('db1');
      const db = await openDB('db1', 1, {
        upgrade(db) {
          db.createObjectStore('store1');
        },
      });
      try {
        await idbPopulate(
          db,
          randomData(opts.dataType, opts.numKeys, opts.valSize),
        );
      } finally {
        db.close();
      }
    },

    async run(bench: Bencher) {
      const db = await openDB('db1');
      try {
        bench.reset();
        const tx = db.transaction('store1', 'readonly', {
          durability: 'relaxed',
        });
        const store = await tx.objectStore('store1');
        const vals = await store.getAll(IDBKeyRange.bound(0, opts.numKeys - 1));
        bench.stop();
        // Use the values to ensure they aren't optimized away.
        console.log(`Read ${vals.length} values`);
      } finally {
        db.close();
      }
    },
  };
}

async function idbPopulate(
  db: IDBPDatabase<unknown>,
  data: (string | Record<string, string> | ArrayBuffer | Blob)[],
) {
  const tx = db.transaction('store1', 'readwrite', {durability: 'relaxed'});
  const store = tx.objectStore('store1');
  await Promise.all(data.map((v, i) => store.put(v, i)));
  await tx.done;
}

export function benchmarkIDBWrite(opts: {
  dataType: RandomDataType;
  group: string;
  valSize: number;
  numKeys: number;
}): {
  name: string;
  group: string;
  byteSize: number;
  setup(): Promise<void>;
  run(bench: Bencher): Promise<void>;
} {
  return {
    name: `idb write tx (${opts.dataType}) ${opts.numKeys}x${xbytes(
      opts.valSize,
      {
        fixed: 0,
        iec: true,
      },
    )}`,
    group: opts.group,
    byteSize: opts.valSize * opts.numKeys,

    async setup() {
      await deleteDB('db1');
      const db = await openDB('db1', 1, {
        upgrade(db) {
          db.createObjectStore('store1');
        },
      });
      db.close();
    },

    async run(bench: Bencher) {
      const db = await openDB('db1');
      try {
        const data = randomData(opts.dataType, opts.numKeys, opts.valSize);
        bench.reset();
        await idbPopulate(db, data);
        bench.stop();
      } finally {
        db.close();
      }
    },
  };
}
