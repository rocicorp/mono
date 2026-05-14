// oxlint-disable no-console
import {deleteDB, type IDBPDatabase, openDB} from 'idb/with-async-ittr';
import xbytes from 'xbytes';
import {afterAll, beforeAll} from 'vitest';
import {bench, describe} from '../../../shared/src/bench.ts';
import {randomData, type RandomDataType} from '../../../shared/src/test-data.ts';

async function idbPopulate(
  db: IDBPDatabase<unknown>,
  data: (string | Record<string, string> | ArrayBuffer | Blob)[],
): Promise<void> {
  const tx = db.transaction('store1', 'readwrite', {durability: 'relaxed'});
  const store = tx.objectStore('store1');
  await Promise.all(data.map((v, i) => store.put(v, i)));
  await tx.done;
}

async function idbPopulateInlineKey(
  db: IDBPDatabase<unknown>,
  storeName: string,
  data: (string | Record<string, string> | ArrayBuffer | Blob)[],
): Promise<void> {
  const tx = db.transaction(storeName, 'readwrite', {durability: 'relaxed'});
  const store = tx.objectStore(storeName);
  await Promise.all(data.map((v, i) => store.put({key: i, value: v})));
  await tx.done;
}

function fmtBytes(bytes: number): string {
  return xbytes(bytes, {fixed: 0, iec: true});
}

type IDBBenchOpts = {
  dataType: RandomDataType;
  group: string;
  valSize: number;
  numKeys: number;
};

function makeIDBReadGetAllBench(opts: IDBBenchOpts) {
  const {dataType, valSize, numKeys} = opts;
  const dbName = 'db1';
  const storeName = 'store1';
  const name = `idb read tx getAll (${dataType}) ${numKeys}x${fmtBytes(valSize)}`;

  describe(name, () => {
    beforeAll(async () => {
      await deleteDB(dbName);
      const db = await openDB(dbName, 1, {
        upgrade(db: IDBDatabase) {
          db.createObjectStore(storeName);
        },
      });
      try {
        await idbPopulate(db, randomData(dataType, numKeys, valSize));
      } finally {
        db.close();
      }
    });

    afterAll(async () => {
      await deleteDB(dbName);
    });

    bench(name, async function* () {
      const db = await openDB(dbName);
      try {
        yield async () => {
          const tx = db.transaction(storeName, 'readonly', {
            durability: 'relaxed',
          });
          const store = tx.objectStore(storeName);
          const values = await store.getAll(
            IDBKeyRange.bound(0, numKeys - 1),
          );
          console.log(`Read ${values.length} values`);
        };
      } finally {
        db.close();
      }
    });
  });
}

function makeIDBReadGetAllGetAllKeysBench(opts: IDBBenchOpts) {
  const {dataType, valSize, numKeys} = opts;
  const dbName = 'db1';
  const storeName = 'store1';
  const name = `idb read tx getAll & getAllKeys (${dataType}) ${numKeys}x${fmtBytes(valSize)}`;

  describe(name, () => {
    beforeAll(async () => {
      await deleteDB(dbName);
      const db = await openDB(dbName, 1, {
        upgrade(db: IDBDatabase) {
          db.createObjectStore('store1');
        },
      });
      try {
        await idbPopulate(db, randomData(dataType, numKeys, valSize));
      } finally {
        db.close();
      }
    });

    afterAll(async () => {
      await deleteDB(dbName);
    });

    bench(name, async function* () {
      const db = await openDB(dbName);
      try {
        yield async () => {
          const tx = db.transaction(storeName, 'readonly', {
            durability: 'relaxed',
          });
          const store = tx.objectStore(storeName);
          const query = IDBKeyRange.bound(0, numKeys - 1);
          const [values, keys] = await Promise.all([
            store.getAll(query),
            store.getAllKeys(query),
          ]);
          console.log(`Read ${values.length} values and ${keys.length} keys`);
        };
      } finally {
        db.close();
      }
    });
  });
}

function makeIDBReadGetBench(opts: IDBBenchOpts) {
  const {dataType, valSize, numKeys} = opts;
  const dbName = 'db1';
  const storeName = 'store1';
  const name = `idb read tx get (${dataType}) ${numKeys}x${fmtBytes(valSize)}`;

  describe(name, () => {
    beforeAll(async () => {
      await deleteDB(dbName);
      const db = await openDB(dbName, 1, {
        upgrade(db: IDBDatabase) {
          db.createObjectStore(storeName);
        },
      });
      try {
        await idbPopulate(db, randomData(dataType, numKeys, valSize));
      } finally {
        db.close();
      }
    });

    afterAll(async () => {
      await deleteDB(dbName);
    });

    bench(name, async function* () {
      const db = await openDB(dbName);
      try {
        yield async () => {
          const tx = db.transaction(storeName, 'readonly', {
            durability: 'relaxed',
          });
          const store = tx.objectStore(storeName);
          const values = await Promise.all(
            Array.from({length: numKeys}, (_, i) => store.get(i)),
          );
          console.log(`Read ${values.length} values`);
        };
      } finally {
        db.close();
      }
    });
  });
}

function makeIDBWriteBench(opts: IDBBenchOpts) {
  const {dataType, valSize, numKeys} = opts;
  const name = `idb write tx (${dataType}) ${numKeys}x${fmtBytes(valSize)}`;

  describe(name, () => {
    beforeAll(async () => {
      await deleteDB('db1');
      const db = await openDB('db1', 1, {
        upgrade(db: IDBDatabase) {
          db.createObjectStore('store1');
        },
      });
      db.close();
    });

    afterAll(async () => {
      await deleteDB('db1');
    });

    bench(name, async function* () {
      const db = await openDB('db1');
      try {
        const data = randomData(dataType, numKeys, valSize);
        yield async () => {
          await idbPopulate(db, data);
        };
      } finally {
        db.close();
      }
    });
  });
}

function makeIDBReadGetWithInlineKeysBench(opts: IDBBenchOpts) {
  const {dataType, valSize, numKeys} = opts;
  const dbName = 'db2';
  const storeName = 'store2';
  const name = `idb read inline tx get (${dataType}) ${numKeys}x${fmtBytes(valSize)}`;

  describe(name, () => {
    beforeAll(async () => {
      await deleteDB(dbName);
      const db = await openDB(dbName, 1, {
        upgrade(db: IDBDatabase) {
          db.createObjectStore(storeName, {keyPath: 'key'});
        },
      });
      try {
        await idbPopulateInlineKey(
          db,
          storeName,
          randomData(dataType, numKeys, valSize),
        );
      } finally {
        db.close();
      }
    });

    afterAll(async () => {
      await deleteDB(dbName);
    });

    bench(name, async function* () {
      const db = await openDB(dbName);
      try {
        yield async () => {
          const tx = db.transaction(storeName, 'readonly', {
            durability: 'relaxed',
          });
          const store = tx.objectStore(storeName);
          const vals = await Promise.all(
            Array.from({length: numKeys}, (_, i) => store.get(i)),
          );
          console.log(`Read ${vals.length} values`);
        };
      } finally {
        db.close();
      }
    });
  });
}

function makeIDBReadGetAllWithInlineKeyBench(opts: IDBBenchOpts) {
  const {dataType, valSize, numKeys} = opts;
  const dbName = 'db2';
  const storeName = 'store2';
  const name = `idb read inline tx getAll (${dataType}) ${numKeys}x${fmtBytes(valSize)}`;

  describe(name, () => {
    beforeAll(async () => {
      await deleteDB(dbName);
      const db = await openDB(dbName, 1, {
        upgrade(db: IDBDatabase) {
          db.createObjectStore(storeName, {keyPath: 'key'});
        },
      });
      try {
        await idbPopulateInlineKey(
          db,
          storeName,
          randomData(dataType, numKeys, valSize),
        );
      } finally {
        db.close();
      }
    });

    afterAll(async () => {
      await deleteDB(dbName);
    });

    bench(name, async function* () {
      const db = await openDB(dbName);
      try {
        yield async () => {
          const tx = db.transaction(storeName, 'readonly', {
            durability: 'relaxed',
          });
          const store = tx.objectStore(storeName);
          const values = await store.getAll(
            IDBKeyRange.bound(0, numKeys - 1),
          );
          console.log(`Read ${values.length} values`);
        };
      } finally {
        db.close();
      }
    });
  });
}

function makeIDBWriteWithInlineKeyBench(opts: IDBBenchOpts) {
  const {dataType, valSize, numKeys} = opts;
  const dbName = 'db2';
  const storeName = 'store2';
  const name = `idb write inline tx (${dataType}) ${numKeys}x${fmtBytes(valSize)}`;

  describe(name, () => {
    beforeAll(async () => {
      await deleteDB(dbName);
      const db = await openDB(dbName, 1, {
        upgrade(db: IDBDatabase) {
          db.createObjectStore(storeName, {keyPath: 'key'});
        },
      });
      db.close();
    });

    afterAll(async () => {
      await deleteDB(dbName);
    });

    bench(name, async function* () {
      const db = await openDB(dbName);
      try {
        const data = randomData(dataType, numKeys, valSize);
        yield async () => {
          await idbPopulateInlineKey(db, storeName, data);
        };
      } finally {
        db.close();
      }
    });
  });
}

const benchmarkFns = [
  makeIDBReadGetBench,
  makeIDBReadGetWithInlineKeysBench,
  makeIDBReadGetAllBench,
  makeIDBReadGetAllGetAllKeysBench,
  makeIDBReadGetAllWithInlineKeyBench,
  makeIDBWriteBench,
  makeIDBWriteWithInlineKeyBench,
];

const kb = 1024;
const mb = kb * kb;
const sizes = [kb, 32 * kb, mb, 10 * mb, 100 * mb];

for (const makeBench of benchmarkFns) {
  for (const numKeys of [1, 10, 100, 1000]) {
    const dataTypes: RandomDataType[] = ['string', 'object', 'arraybuffer'];
    for (const dataType of dataTypes) {
      const group = dataType === 'arraybuffer' ? 'idb' : 'idb-extras';
      for (const valSize of sizes) {
        if (valSize > 10 * mb) {
          if (numKeys > 1) continue;
        } else if (valSize >= mb) {
          if (numKeys > 10) continue;
        }
        makeBench({group, dataType, numKeys, valSize});
      }
    }
  }
}
