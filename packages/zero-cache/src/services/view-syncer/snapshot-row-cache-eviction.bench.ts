import {performance} from 'node:perf_hooks';
import {describe, expect, test} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  DEFAULT_MAX_SNAPSHOT_ROW_CACHE_ENTRIES,
  SnapshotRowCache,
} from './snapshot-row-cache.ts';

// Run: pnpm --filter zero-cache run bench snapshot-row-cache-eviction
// Fill the default cache, then measure four successive batches of misses
// without clearing it. Report batch latency; do not assert timing thresholds.
// The large-cache control does the same key construction and SQLite reads but
// never evicts, isolating eviction overhead from other cache overhead.
const BATCH_SIZE = DEFAULT_MAX_SNAPSHOT_ROW_CACHE_ENTRIES;
const BATCHES = 4;
const REPEATS = 3;
const SQL = 'SELECT id, value FROM rows WHERE id=?';
type Row = {id: bigint; value: string};

describe('snapshot row cache sustained eviction', () => {
  const recorder = createManualBenchmarkRecorder();
  const variants = [
    ['direct SQLite', undefined],
    ['cache disabled', 0],
    ['cache without eviction', BATCH_SIZE * (BATCHES + 1)],
    ['default cache with eviction', DEFAULT_MAX_SNAPSHOT_ROW_CACHE_ENTRIES],
  ] as const;

  for (const [name, maxEntries] of variants) {
    test(name, {timeout: 60_000}, () => {
      const db = new Database(createSilentLogContext(), ':memory:');
      try {
        db.exec(`
          CREATE TABLE rows (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
          WITH RECURSIVE ids(id) AS (
            SELECT 0 UNION ALL SELECT id + 1 FROM ids WHERE id < ${BATCH_SIZE - 1}
          )
          INSERT INTO rows SELECT id, 'value' FROM ids;
        `);
        const statement = db.prepare(SQL);
        statement.safeIntegers(true);
        const samples = Array.from({length: BATCHES}, () => [] as number[]);
        for (let repeat = 0; repeat < REPEATS; repeat++) {
          const cache =
            maxEntries === undefined
              ? undefined
              : new SnapshotRowCache(maxEntries);
          // Each batch represents a new snapshot version of the same rows.
          // Every read misses, while the indexed SQLite working set stays warm.
          for (let batch = 0; batch <= BATCHES; batch++) {
            const tag = `n:${batch}`;
            let checksum = 0;
            const start = performance.now();
            for (let id = 0; id < BATCH_SIZE; id++) {
              const read = () => statement.get<Row>(id);
              const row = cache
                ? cache.getOrRead(tag, SQL, 'get', [id], read)
                : read();
              checksum += Number(row.id);
            }
            const elapsed = performance.now() - start;
            expect(checksum).toBe((BATCH_SIZE * (BATCH_SIZE - 1)) / 2);
            if (cache) {
              expect(cache.stats()).toEqual({
                hits: 0,
                misses: (batch + 1) * BATCH_SIZE,
                size: Math.min((batch + 1) * BATCH_SIZE, cache.maxEntries),
              });
            }
            // Batch zero fills the cache and warms SQLite outside measurement.
            if (batch > 0) {
              samples[batch - 1].push(elapsed);
            }
          }
        }
        for (let batch = 0; batch < BATCHES; batch++) {
          recorder.recordLatency(
            `${name}, batch ${batch + 1} (${BATCH_SIZE} reads)`,
            samples[batch],
          );
        }
      } finally {
        db.close();
      }
    });
  }
});
