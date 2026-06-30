import postgres from 'postgres';
import type {BenchmarkConfig} from './config.ts';
import {sleep} from './util.ts';

export type BenchmarkDB = postgres.Sql;

export function connectBenchmarkDB(url: string): BenchmarkDB {
  return postgres(url, {
    idle_timeout: 0,
    connect_timeout: 30,
    max_lifetime: null,
    onnotice: () => undefined,
  });
}

export async function waitForPostgres(
  url: string,
  timeoutMs: number,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    const sql = postgres(url, {
      max: 1,
      idle_timeout: 1,
      connect_timeout: 5,
      onnotice: () => undefined,
    });
    try {
      await sql`SELECT 1`;
      await sql.end();
      return;
    } catch (error) {
      lastError = error;
      await sql.end().catch(() => undefined);
      await sleep(500);
    }
  }
  throw new Error(
    `Timed out waiting for PostgreSQL after ${timeoutMs}ms: ${String(lastError)}`,
  );
}

export async function resetBenchmarkDatabase(
  sql: BenchmarkDB,
  config: BenchmarkConfig,
): Promise<void> {
  const appID = config.zero.appID;
  const schemas = [appID, `${appID}_0`, `${appID}_0/cvr`, `${appID}_0/cdc`];

  for (const schema of schemas) {
    await sql`DROP SCHEMA IF EXISTS ${sql(schema)} CASCADE`;
  }

  await sql`DROP TABLE IF EXISTS zero_throughput_event CASCADE`;
  await sql`
    CREATE TABLE zero_throughput_event (
      id text PRIMARY KEY,
      profile text NOT NULL,
      shard integer NOT NULL,
      bucket integer NOT NULL,
      seq bigint NOT NULL,
      payload jsonb NOT NULL,
      written_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
    )
  `;
  await sql`
    CREATE UNIQUE INDEX zero_throughput_event_seq_idx
    ON zero_throughput_event (seq)
  `;
  await sql`
    CREATE INDEX zero_throughput_event_bucket_seq_idx
    ON zero_throughput_event (bucket, seq DESC)
  `;
}
