// Benchmarks initial sync against an existing zmail-style PostgreSQL database.

import {copyFileSync, existsSync, mkdirSync} from 'node:fs';
import {dirname, join} from 'node:path';
import {afterAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {createLogContext} from '../../../shared/src/logging.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {initReplica} from '../services/change-source/common/replica-schema.ts';
import {
  initialSync,
  type InitialSyncOptions,
} from '../services/change-source/pg/initial-sync.ts';
import {dropShard} from '../services/change-source/pg/schema/shard.ts';
import {DbFile} from '../test/lite.ts';
import {connectPgClient} from '../types/pg.ts';
import type {ShardConfig} from '../types/shards.ts';
import {id} from '../types/sql.ts';

const DEFAULT_UPSTREAM_URI = 'postgres://postgres:pass@localhost:5547/zmail';
const TEST_TIMEOUT_MS = 12 * 60 * 60 * 1000;
const APP_ID_PREFIX = 'zmail_bench';

const lc =
  process.env.ZERO_BENCH_LOG === '0'
    ? createSilentLogContext()
    : createLogContext(
        {log: {level: 'info', format: 'text'}},
        {bench: 'zmail-initial-sync-v1.7'},
      );

const replicaFiles: DbFile[] = [];
const upstreamURI = process.env.ZMAIL_PG_URI ?? DEFAULT_UPSTREAM_URI;
const replicaOutputPath = process.env.ZMAIL_REPLICA_OUTPUT_PATH;
const replicaOutputDir = process.env.ZMAIL_REPLICA_OUTPUT_DIR;

function envNumber(name: string, defaultValue: number): number {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return defaultValue;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${name} must be a finite number, got ${value}`);
  }
  return parsed;
}

function envOptionalNumber(name: string): number | undefined {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return undefined;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`${name} must be a finite number, got ${value}`);
  }
  return parsed;
}

function envBool(name: string, defaultValue = false): boolean {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return defaultValue;
  }
  return value === '1' || value === 'true';
}

function appID(run: number): string {
  const suffix = process.env.ZMAIL_APP_SUFFIX ?? Date.now().toString(36);
  return `${APP_ID_PREFIX}_${suffix}_${run}`;
}

async function cleanupShard(appID: string) {
  const sql = await connectPgClient(lc, upstreamURI, 'zmail-cleanup', {max: 1});
  try {
    await sql.unsafe(dropShard(appID, 0));
    await sql.unsafe(`DROP SCHEMA IF EXISTS ${id(appID)} CASCADE`);
    await sql /*sql*/ `
      SELECT pg_drop_replication_slot(slot_name)
      FROM pg_replication_slots
      WHERE slot_name LIKE ${`${appID.replaceAll('_', '\\_')}_0_%`} ESCAPE '\\'
        AND active = false;
    `;
  } finally {
    await sql.end();
  }
}

function replicaRowCount(replicaPath: string): number {
  const db = new Database(lc, replicaPath, {readonly: true});
  try {
    let total = 0;
    for (const table of ['email_content', 'email_metadata', 'email_address']) {
      const row = db
        .prepare(`SELECT count(*) AS n FROM ${table}`)
        .get<{n: number}>();
      total += row.n;
    }
    return total;
  } finally {
    db.close();
  }
}

function preserveReplica(replicaPath: string, app: string) {
  const outputPath =
    replicaOutputPath ??
    (replicaOutputDir === undefined
      ? undefined
      : join(replicaOutputDir, `zmail-initial-sync-${app}.db`));
  if (outputPath === undefined) {
    return;
  }
  mkdirSync(dirname(outputPath), {recursive: true});
  for (const suffix of ['', '-wal', '-shm']) {
    if (existsSync(`${replicaPath}${suffix}`)) {
      copyFileSync(`${replicaPath}${suffix}`, `${outputPath}${suffix}`);
    }
  }
  lc.info?.(`zmail preserved replica at ${outputPath}`);
}

afterAll(() => {
  for (const file of replicaFiles) {
    file.delete();
  }
});

describe('zero-cache/zmail initial-sync throughput', () => {
  test('existing zmail payload', {timeout: TEST_TIMEOUT_MS}, async () => {
    const warmupReps = envNumber('ZMAIL_WARMUP_REPS', 0);
    const reps = envNumber('ZMAIL_REPS', 1);
    const payloadMiB = envOptionalNumber('ZMAIL_PAYLOAD_MIB');
    const expectedRows = envOptionalNumber('ZMAIL_EXPECTED_ROWS');
    const options: InitialSyncOptions = {
      tableCopyWorkers: envNumber('ZMAIL_TABLE_COPY_WORKERS', 5),
      profileCopy: envBool('ZMAIL_PROFILE_COPY'),
      textCopy: envBool('ZMAIL_TEXT_COPY'),
    };

    lc.info?.(`zmail benchmark config`, {
      upstreamURI,
      warmupReps,
      reps,
      payloadMiB,
      expectedRows,
      options,
    });

    for (let rep = 0; rep < warmupReps + reps; rep++) {
      const app = appID(rep);
      await cleanupShard(app);
      const replicaDbFile = new DbFile(`zmail-initial-sync-${app}`);
      replicaFiles.push(replicaDbFile);
      const shard: ShardConfig = {appID: app, shardNum: 0, publications: []};

      const start = performance.now();
      try {
        await initReplica(
          lc,
          `zmail-initial-sync-${app}`,
          replicaDbFile.path,
          (log, tx) =>
            initialSync(log, shard, tx, upstreamURI, options, {
              bench: 'zmail-initial-sync-v1.7',
              rep,
            }),
        );
      } finally {
        await cleanupShard(app).catch(e =>
          lc.warn?.(`zmail cleanup failed for ${app}`, e),
        );
      }

      const elapsed = performance.now() - start;
      const rows = replicaRowCount(replicaDbFile.path);
      if (expectedRows !== undefined) {
        expect(rows).toBe(expectedRows);
      } else {
        expect(rows).toBeGreaterThan(0);
      }
      preserveReplica(replicaDbFile.path, app);
      lc.info?.(
        `zmail initial sync rep ${rep} copied ${rows} rows in ${elapsed.toFixed(3)} ms`,
      );
      if (payloadMiB !== undefined) {
        lc.info?.(
          `zmail initial sync rep ${rep} throughput ${(payloadMiB / (elapsed / 1000)).toFixed(1)} MiB/s`,
        );
      }
      lc.info?.(
        `zmail initial sync memory after rep ${rep}`,
        process.memoryUsage(),
      );
    }
  });
});
