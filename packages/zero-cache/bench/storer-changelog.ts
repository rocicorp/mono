/* oxlint-disable no-console */
import {performance} from 'node:perf_hooks';
import {PostgreSqlContainer} from '@testcontainers/postgresql';
import postgres from 'postgres';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {
  ensureReplicationConfig,
  setupCDCTables,
} from '../src/services/change-streamer/schema/tables.ts';
import {postgresTypeConfig, type PostgresDB} from '../src/types/pg.ts';
import {cdcSchema, type ShardID} from '../src/types/shards.ts';

type Mode = 'single' | 'batched';

type ChangeLogInsert = {
  watermark: string;
  precommit: string | null;
  pos: number;
  change: string;
};

type Result = {
  readonly mode: Mode;
  readonly elapsedMs: number;
  readonly changes: number;
  readonly changesPerSec: number;
  readonly insertStatements: number;
  readonly statementReductionPct: number;
};

type Summary = {
  readonly name: 'storer-changelog-insert';
  readonly generatedAt: string;
  readonly txCount: number;
  readonly changesPerTx: number;
  readonly batchSize: number;
  readonly results: Result[];
};

const lc = createSilentLogContext();
const shard: ShardID = {appID: 'bench', shardNum: 0};
const schema = cdcSchema(shard);

const txCount = envInt('ZERO_STORER_CHANGELOG_TX', 200);
const changesPerTx = envInt('ZERO_STORER_CHANGELOG_CHANGES_PER_TX', 100);
const batchSize = envInt('ZERO_STORER_CHANGELOG_BATCH', 100);

const container = await new PostgreSqlContainer(
  process.env.ZERO_STORER_CHANGELOG_PG_IMAGE ?? 'postgres:17',
).start();

try {
  const db = postgres(container.getConnectionUri(), {
    ...postgresTypeConfig({sendStringAsJson: true}),
  });
  try {
    const single = await runMode(db, 'single');
    const batched = await runMode(db, 'batched');
    const summary: Summary = {
      name: 'storer-changelog-insert',
      generatedAt: new Date().toISOString(),
      txCount,
      changesPerTx,
      batchSize,
      results: [single, batched],
    };

    for (const result of summary.results) {
      console.log(
        `${result.mode}: ${formatRate(result.changesPerSec)} changes/s | ` +
          `${result.elapsedMs.toFixed(1)} ms | ` +
          `${result.insertStatements} INSERT statements`,
      );
    }
    console.log(JSON.stringify(summary));
  } finally {
    await db.end();
  }
} finally {
  await container.stop();
}

async function runMode(db: PostgresDB, mode: Mode): Promise<Result> {
  await db`DROP SCHEMA IF EXISTS ${db(schema)} CASCADE`;
  await db.begin(tx => setupCDCTables(lc, tx, shard));
  await ensureReplicationConfig(
    lc,
    db,
    {
      replicaVersion: '00',
      publications: [],
      watermark: '00',
    },
    shard,
    true,
  );

  let insertStatements = 0;
  const started = performance.now();
  for (let txNum = 1; txNum <= txCount; txNum++) {
    const watermark = txNum.toString(36).padStart(12, '0');
    const rows = makeTransaction(watermark, changesPerTx);
    await db.begin(async sql => {
      if (mode === 'single') {
        for (const row of rows) {
          insertStatements++;
          await sql`INSERT INTO ${sql(`${schema}.changeLog`)} ${sql(row)}`;
        }
      } else {
        for (let i = 0; i < rows.length; i += batchSize) {
          insertStatements++;
          await sql`INSERT INTO ${sql(`${schema}.changeLog`)} ${sql(
            rows.slice(i, i + batchSize),
          )}`;
        }
      }
      await sql`UPDATE ${sql(`${schema}.replicationState`)}
        SET "lastWatermark" = ${watermark}`;
    });
  }
  const elapsedMs = performance.now() - started;
  const changes = txCount * (changesPerTx + 2);
  const singleInsertStatements = changes;
  return {
    mode,
    elapsedMs,
    changes,
    changesPerSec: changes / (elapsedMs / 1000),
    insertStatements,
    statementReductionPct:
      ((singleInsertStatements - insertStatements) / singleInsertStatements) *
      100,
  };
}

function makeTransaction(
  watermark: string,
  changes: number,
): ChangeLogInsert[] {
  const rows: ChangeLogInsert[] = [
    {
      watermark,
      precommit: null,
      pos: 0,
      change: JSON.stringify({tag: 'begin'}),
    },
  ];
  for (let i = 0; i < changes; i++) {
    rows.push({
      watermark,
      precommit: null,
      pos: i + 1,
      change: JSON.stringify({tag: 'insert', new: {id: `${watermark}-${i}`}}),
    });
  }
  rows.push({
    watermark,
    precommit: watermark,
    pos: changes + 1,
    change: JSON.stringify({tag: 'commit'}),
  });
  return rows;
}

function envInt(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value === '') {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid integer ${name}=${value}`);
  }
  return parsed;
}

function formatRate(value: number): string {
  return value.toLocaleString('en-US', {maximumFractionDigits: 1});
}
