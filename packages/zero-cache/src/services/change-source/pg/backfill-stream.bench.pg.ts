// Benchmarks binary backfill throughput from a generated PostgreSQL fixture
// through complete backfill message consumption.

import {afterEach, describe, expect} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import {getConnectionURI, type PgTest, test} from '../../../test/db.ts';
import {
  BENCHMARK_FIXTURE_PUBLICATION,
  type InitialSyncBenchmarkFixture,
  benchmarkProductionPayload,
  initialSyncBenchmarkPayloadMB,
  makeBenchmarkFixtureRow,
  setupInitialSyncBenchmarkFixture,
} from '../../../test/pg-bench.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {BackfillRequest} from '../protocol/current.ts';
import {streamBackfill} from './backfill-stream.ts';
import {
  getPublicationInfo,
  type PublishedTableWithReplicaIdentity,
} from './schema/published.ts';

const PROFILE_ENV = 'ZERO_BACKFILL_BENCH_PROFILE';

type Profile = {
  fixture: InitialSyncBenchmarkFixture;
  warmupReps: number;
  reps: number;
  timeoutMs: number;
};

const PROFILES = {
  'mixed-regression': {
    fixture: {fixture: 'mixed', rows: 250_000},
    warmupReps: 1,
    reps: 5,
    timeoutMs: 3_600_000,
  },
  'wide-text-narrow': {
    fixture: {fixture: 'wide-text', rows: 250_000, payloadBytes: 128},
    warmupReps: 1,
    reps: 10,
    timeoutMs: 3_600_000,
  },
  'large-payload-scaled': {
    fixture: {fixture: 'large-payload', rows: 2_000, payloadBytes: 275_000},
    warmupReps: 1,
    reps: 10,
    timeoutMs: 3_600_000,
  },
} as const satisfies Record<string, Profile>;

const profileName = process.env[PROFILE_ENV] ?? 'mixed-regression';
if (!Object.hasOwn(PROFILES, profileName)) {
  throw new Error(
    `${PROFILE_ENV} must be one of ${Object.keys(PROFILES).join(', ')}; got ${JSON.stringify(profileName)}`,
  );
}
const profile = PROFILES[profileName as keyof typeof PROFILES];

const lc = createSilentLogContext();
const benchmarkRecorder = createManualBenchmarkRecorder();
const CONNECTION_CLOSE_TIMEOUT_MS = 10_000;

let cleanup: (() => Promise<void> | void)[] = [];

async function runCleanup() {
  for (const fn of cleanup.reverse()) {
    await fn();
  }
  cleanup = [];
}

afterEach(async () => {
  await runCleanup();
});

function makeBackfillRequest(
  table: PublishedTableWithReplicaIdentity,
): BackfillRequest {
  const rowKey = Object.fromEntries(
    table.replicaIdentityColumns.map(column => [
      column,
      {attNum: table.columns[column].pos},
    ]),
  );
  const columns = Object.fromEntries(
    Object.entries(table.columns).map(([column, spec]) => [
      column,
      {attNum: spec.pos},
    ]),
  );

  return {
    table: {
      schema: table.schema,
      name: table.name,
      metadata: {
        schemaOID: must(table.schemaOID),
        relationOID: table.oid,
        rowKey,
      },
    },
    columns,
  };
}

type BackfillSample = {
  schema: string;
  table: string;
  columns: string[];
  first: readonly unknown[];
  last: readonly unknown[];
};

async function closeBackfillConnections(upstream: PostgresDB) {
  await upstream`
    SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
     WHERE datname = current_database()
       AND application_name = 'zero-backfill-stream'`;

  const deadline = performance.now() + CONNECTION_CLOSE_TIMEOUT_MS;
  for (;;) {
    const [{count}] = await upstream<{count: number}[]>`
      SELECT count(*)::int AS count
        FROM pg_stat_activity
       WHERE datname = current_database()
         AND application_name = 'zero-backfill-stream'`;
    if (count === 0) {
      return;
    }
    if (performance.now() >= deadline) {
      throw new Error(`backfill-stream connections did not close`);
    }
    await sleep(10);
  }
}

function variablePayloadBytes() {
  if (profile.fixture.fixture === 'mixed') {
    throw new Error(`mixed benchmark tables have variable payload sizes`);
  }
  return profile.fixture.payloadBytes;
}

function validateSampleRow(sample: BackfillSample, row: readonly unknown[]) {
  const value = Object.fromEntries(
    sample.columns.map((column, i) => [column, row[i]]),
  );

  switch (`${sample.schema}.${sample.table}`) {
    case 'public.bench_rows': {
      const id = value.id as number;
      const expected = makeBenchmarkFixtureRow(id);
      expect(expected.table).toBe(sample.table);
      expect(value).toEqual(expected.row);
      return;
    }
    case 'public.bench_lookup': {
      const id = value.id as number;
      const expected = makeBenchmarkFixtureRow(id);
      expect(expected.table).toBe(sample.table);
      expect(value).toEqual({
        ...expected.row,
        active: Number(expected.row.active),
      });
      return;
    }
    case 'public.bench_wide': {
      const id = value.id as number;
      const expected = makeBenchmarkFixtureRow(id);
      expect(expected.table).toBe(sample.table);
      expect(value).toEqual(expected.row);
      return;
    }
    case 'public.bench_composite': {
      const id = (value.seq as number) * 1_000 + (value.account_id as number);
      const expected = makeBenchmarkFixtureRow(id);
      expect(expected.table).toBe(sample.table);
      expect(value).toEqual(expected.row);
      return;
    }
    case 'public.bench_wide_text': {
      const id = value.id as string;
      const rowNumber = Number(id.slice('wide-text-'.length));
      const payload = value.large_text as string;
      expect(id).toBe(`wide-text-${rowNumber}`);
      expect(payload).toBe(
        benchmarkProductionPayload(
          'wide-text',
          rowNumber,
          variablePayloadBytes(),
        ),
      );
      return;
    }
    case 'benchmark.bench_large_payload': {
      const id = value.id as string;
      const rowNumber = Number(id.slice('payload-'.length));
      const {data} = JSON.parse(value.payload as string) as {data: string};
      expect(id).toBe(`payload-${rowNumber}`);
      expect(data).toBe(
        benchmarkProductionPayload(
          'large-payload',
          rowNumber,
          variablePayloadBytes(),
        ),
      );
      return;
    }
    default:
      throw new Error(
        `unexpected benchmark table ${sample.schema}.${sample.table}`,
      );
  }
}

describe('zero-cache/backfill throughput', () => {
  test(
    `${profileName} generated fixture payload MB/sec`,
    {timeout: profile.timeoutMs},
    async ({testDBs}: PgTest) => {
      const samples: number[] = [];
      const fixturePayloadMB = initialSyncBenchmarkPayloadMB(profile.fixture);

      for (let rep = 0; rep < profile.warmupReps + profile.reps; rep++) {
        const upstream = await testDBs.create(
          `backfill_bench_${profileName.replaceAll('-', '_')}_${rep}`,
        );
        cleanup.push(async () => {
          await testDBs.drop(upstream);
        });

        await setupInitialSyncBenchmarkFixture(
          upstream,
          profile.fixture,
          BENCHMARK_FIXTURE_PUBLICATION,
        );
        const {tables} = await getPublicationInfo(upstream, [
          BENCHMARK_FIXTURE_PUBLICATION,
        ]);
        const requests = tables
          .toSorted((a, b) =>
            `${a.schema}.${a.name}`.localeCompare(`${b.schema}.${b.name}`),
          )
          .map(makeBackfillRequest);

        let streamedRows = 0;
        let elapsed = 0;
        const completions: {
          schema: string;
          table: string;
          rowKey: string[];
          columns: string[];
          watermark: string;
          rows: number;
          totalRows: number;
        }[] = [];
        const samplesByTable: BackfillSample[] = [];
        for (const request of requests) {
          let sample: BackfillSample | undefined;
          const start = performance.now();
          for await (const {message} of streamBackfill(
            lc,
            getConnectionURI(upstream),
            {
              slot: 'backfill_bench',
              publications: [BENCHMARK_FIXTURE_PUBLICATION],
            },
            request,
          )) {
            if (message.tag === 'backfill') {
              streamedRows += message.rowValues.length;
              if (message.rowValues.length > 0) {
                const columns = [
                  ...message.relation.rowKey.columns,
                  ...message.columns,
                ];
                sample ??= {
                  schema: message.relation.schema,
                  table: message.relation.name,
                  columns,
                  first: message.rowValues[0],
                  last: message.rowValues[0],
                };
                sample.last = message.rowValues.at(-1)!;
              }
            } else {
              const status = must(message.status);
              completions.push({
                schema: message.relation.schema,
                table: message.relation.name,
                rowKey: message.relation.rowKey.columns,
                columns: message.columns,
                watermark: message.watermark,
                rows: status.rows,
                totalRows: status.totalRows,
              });
            }
          }
          // streamBackfill starts closing its client asynchronously. Exclude
          // that close from every sample and prevent overlap with the next one.
          elapsed += performance.now() - start;
          samplesByTable.push(must(sample));
          await closeBackfillConnections(upstream);
        }

        expect(requests.length).toBeGreaterThan(0);
        expect(completions).toHaveLength(requests.length);
        expect(samplesByTable).toHaveLength(requests.length);
        expect(streamedRows).toBe(profile.fixture.rows);
        expect(
          completions.reduce((total, completion) => total + completion.rows, 0),
        ).toBe(profile.fixture.rows);
        for (const [i, completion] of completions.entries()) {
          const request = requests[i];
          expect(completion.rows).toBe(completion.totalRows);
          expect(completion.schema).toBe(request.table.schema);
          expect(completion.table).toBe(request.table.name);
          expect(completion.watermark.length).toBeGreaterThan(0);
          expect(
            new Set([...completion.rowKey, ...completion.columns]),
          ).toEqual(new Set(Object.keys(request.columns)));
        }
        for (const sample of samplesByTable) {
          validateSampleRow(sample, sample.first);
          validateSampleRow(sample, sample.last);
        }
        if (rep >= profile.warmupReps) {
          samples.push(elapsed);
        }
        await runCleanup();
      }

      benchmarkRecorder.recordThroughput(
        `zero-cache/backfill ${profileName} generated fixture payload MB`,
        samples,
        fixturePayloadMB,
      );
    },
  );
});
