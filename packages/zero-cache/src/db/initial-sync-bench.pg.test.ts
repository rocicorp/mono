/**
 * Benchmarks initial sync against an external PostgreSQL database.
 *
 * Usage:
 *   ZERO_UPSTREAM_DB="postgresql://user:password@127.0.0.1:6434/postgres" \
 *     npm --workspace=zero-cache run test -- --run initial-sync-bench
 *
 * Requires a running PG with data (e.g. zbugs with gigabugs dataset).
 * Skipped automatically if ZERO_UPSTREAM_DB is not set.
 */
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {nanoid} from 'nanoid/non-secure';
import {Database} from '../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../db/delete-lite-db.ts';
import {initReplica} from '../services/change-source/common/replica-schema.ts';
import {initialSync} from '../services/change-source/pg/initial-sync.ts';
import type {ShardConfig} from '../types/shards.ts';
import {pgClient} from '../types/pg.ts';

const UPSTREAM_DB = process.env['ZERO_UPSTREAM_DB'];

describe.skipIf(!UPSTREAM_DB)('initial-sync-bench', () => {
  test(
    'initial sync from external PG',
    {timeout: 3_600_000}, // 1 hour
    async () => {
      const upstreamURI = UPSTREAM_DB!;
      const lc = createSilentLogContext();

      // Count rows in all user tables for the report.
      const sql = pgClient(lc, upstreamURI);
      const tables = await sql<{table: string; rows: bigint}[]>`
        SELECT schemaname || '.' || relname AS table,
               n_live_tup AS rows
        FROM pg_stat_user_tables
        ORDER BY n_live_tup DESC
      `;
      const totalRows = tables.reduce((sum, t) => sum + Number(t.rows), 0);

      // oxlint-disable-next-line no-console
      console.log(`\nUpstream tables:`);
      for (const t of tables) {
        // oxlint-disable-next-line no-console
        console.log(`  ${t.table}: ${Number(t.rows).toLocaleString()} rows`);
      }
      // oxlint-disable-next-line no-console
      console.log(`  TOTAL: ${totalRows.toLocaleString()} rows\n`);

      // Discover publications from PG.
      const pubs = await sql<{pubname: string}[]>`
        SELECT pubname FROM pg_publication
        WHERE pubname NOT LIKE '\\_%'
      `;
      const publications = pubs.map(p => p.pubname);
      await sql.end();
      // oxlint-disable-next-line no-console
      console.log(`Publications: [${publications}]`);

      const shard: ShardConfig = {
        appID: 'bench',
        shardNum: 0,
        publications,
      };

      const replicaPath = join(tmpdir(), `initial-sync-bench-${nanoid()}.db`);

      try {
        const start = performance.now();

        await initReplica(lc, 'initial-sync-bench', replicaPath, (log, tx) =>
          initialSync(
            log,
            shard,
            tx,
            upstreamURI,
            {tableCopyWorkers: 4},
            {
              benchmark: true,
            },
          ),
        );

        const elapsed = performance.now() - start;

        // Check replica size.
        const replica = new Database(lc, replicaPath);
        const [{page_count: pages}] = replica.pragma<{page_count: number}>(
          'page_count',
        );
        const [{page_size: pageSize}] = replica.pragma<{page_size: number}>(
          'page_size',
        );
        const sizeMB = (pages * pageSize) / (1024 * 1024);
        replica.close();

        const fmt = (ms: number) =>
          ms >= 60_000
            ? `${(ms / 60_000).toFixed(1)}min`
            : ms >= 1000
              ? `${(ms / 1000).toFixed(2)}s`
              : `${ms.toFixed(0)}ms`;

        const rowsPerSec = (totalRows / elapsed) * 1000;
        const rateStr =
          rowsPerSec >= 1000
            ? `${(rowsPerSec / 1000).toFixed(1)}K rows/s`
            : `${rowsPerSec.toFixed(0)} rows/s`;

        // oxlint-disable-next-line no-console
        console.log(`\n--- Initial Sync Benchmark ---`);
        // oxlint-disable-next-line no-console
        console.log(`  Total time:    ${fmt(elapsed)}`);
        // oxlint-disable-next-line no-console
        console.log(`  Rows synced:   ${totalRows.toLocaleString()}`);
        // oxlint-disable-next-line no-console
        console.log(`  Rate:          ${rateStr}`);
        // oxlint-disable-next-line no-console
        console.log(`  Replica size:  ${sizeMB.toFixed(1)} MB\n`);

        expect(elapsed).toBeGreaterThan(0);
      } finally {
        deleteLiteDB(replicaPath);
      }
    },
  );
});
