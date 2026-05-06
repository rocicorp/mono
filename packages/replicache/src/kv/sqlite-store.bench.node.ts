/**
 * Benchmarks the cost of batching vs unbatched KV reads, simulating the
 * JS-to-native bridge latency present in op-sqlite / expo-sqlite on React Native.
 *
 * The SerialBridge queues concurrent calls one-at-a-time (the real bridge is
 * serial), so Promise.all of N gets still costs N × BRIDGE_LATENCY_MS without
 * batching. The batched case crosses the bridge once with a single IN query.
 *
 * Run with:
 *   node node_modules/.bin/vitest bench --run --config vitest.config.node.ts sqlite-store
 */
import {existsSync, unlinkSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import open from '@rocicorp/zero-sqlite3';
import {bench, describe} from 'vitest';

// Simulated one-way bridge latency in ms. op-sqlite on a simulator is
// typically 0.3–1ms per call.
const BRIDGE_LATENCY_MS = 0.5;

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

/**
 * Models a serial JS-to-native bridge: no matter how many callers invoke
 * cross() concurrently, they are queued and execute one at a time.
 */
class SerialBridge {
  #tail: Promise<void> = Promise.resolve();

  cross(): Promise<void> {
    const next = this.#tail.then(() => sleep(BRIDGE_LATENCY_MS));
    this.#tail = next.then(
      () => {},
      () => {},
    );
    return next;
  }
}

function makeDb(keyCount: number): {
  db: open.Database;
  keys: string[];
  getStmt: open.Statement;
  inStmt: open.Statement;
} {
  const dbFile = join(tmpdir(), `replicache-kv-bench-${keyCount}.db`);
  if (existsSync(dbFile)) unlinkSync(dbFile);

  const db = open(dbFile);
  db.exec(`PRAGMA journal_mode = WAL`);
  db.exec(`PRAGMA synchronous = NORMAL`);
  db.exec(`
    CREATE TABLE IF NOT EXISTS entry (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    ) WITHOUT ROWID
  `);

  const keys = Array.from({length: keyCount}, (_, i) => `key${i}`);
  const insert = db.prepare(
    'INSERT OR REPLACE INTO entry (key, value) VALUES (?, ?)',
  );
  for (const key of keys) {
    insert.run(key, JSON.stringify(`value-for-${key}`));
  }

  const getStmt = db.prepare('SELECT value FROM entry WHERE key = ?');
  const placeholders = keys.map(() => '?').join(', ');
  const inStmt = db.prepare(
    `SELECT key, value FROM entry WHERE key IN (${placeholders})`,
  );

  return {db, keys, getStmt, inStmt};
}

const benchOpts = {
  time: 3000,
  warmupTime: 500,
};

for (const keyCount of [1, 5, 20]) {
  // DB is set up at module scope so beforeAll/afterAll aren't needed.
  const {keys, getStmt, inStmt} = makeDb(keyCount);

  describe(`${keyCount} keys, ${BRIDGE_LATENCY_MS}ms bridge latency`, () => {
    // -------------------------------------------------------------------------
    // Unbatched sequential: await each get in turn — N bridge crossings, serial.
    // -------------------------------------------------------------------------
    bench(
      'unbatched sequential gets',
      async () => {
        const bridge = new SerialBridge();
        for (const key of keys) {
          await bridge.cross();
          getStmt.all(key);
        }
      },
      benchOpts,
    );

    // -------------------------------------------------------------------------
    // Unbatched concurrent: Promise.all of N gets — still N bridge crossings
    // because the bridge is serial (they queue up behind each other).
    // -------------------------------------------------------------------------
    bench(
      'unbatched concurrent gets (Promise.all)',
      async () => {
        const bridge = new SerialBridge();
        await Promise.all(
          keys.map(async key => {
            await bridge.cross();
            getStmt.all(key);
          }),
        );
      },
      benchOpts,
    );

    // -------------------------------------------------------------------------
    // Batched: one bridge crossing, one SELECT … WHERE key IN (?, …) query.
    // This is what the proposed microtask-delay batching optimisation achieves.
    // -------------------------------------------------------------------------
    bench(
      'batched gets (single IN query)',
      async () => {
        const bridge = new SerialBridge();
        await bridge.cross();
        inStmt.all(...keys);
      },
      benchOpts,
    );
  });
}
