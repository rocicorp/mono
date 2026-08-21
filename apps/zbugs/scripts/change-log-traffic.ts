/* oxlint-disable no-console */

import '../../../packages/shared/src/dotenv.ts';

import {performance} from 'node:perf_hooks';
import {parseArgs} from 'node:util';
import postgres from 'postgres';

type Fixture = {
  creatorID: string;
  projectID: string;
};

type StageResult = {
  targetTransactionsPerSecond: number;
  durationSeconds: number;
  transactions: number;
  mutations: number;
  elapsedSeconds: number;
  actualTransactionsPerSecond: number;
  latencyMs: {
    p50: number;
    p95: number;
    p99: number;
    max: number;
  };
};

const USAGE = `
Drive repeatable PostgreSQL traffic through the zbugs issue table.

Usage:
  node scripts/change-log-traffic.ts [options]

Options:
  --rates <list>               Comma-separated transaction rates. Default: 5,25,100
  --duration-seconds <number>  Duration of each stage. Default: 5
  --repeat <number>            Number of times to run all stages. Default: 1
  --concurrency <number>       Maximum in-flight transactions. Default: 32
  --payload-bytes <number>     Approximate issue description size. Default: 256
  --json                       Print results as JSON
  --help                       Show this help

Each transaction inserts, updates, and deletes one issue. It commits three row
mutations and leaves no traffic rows in the database.
`;

function numberInRange(
  name: string,
  value: string,
  min: number,
  max: number,
): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < min || parsed > max) {
    throw new Error(`${name} must be between ${min} and ${max}`);
  }
  return parsed;
}

function integerInRange(
  name: string,
  value: string,
  min: number,
  max: number,
): number {
  const parsed = numberInRange(name, value, min, max);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${name} must be an integer`);
  }
  return parsed;
}

function percentile(sorted: readonly number[], percentage: number): number {
  if (sorted.length === 0) {
    return 0;
  }
  return sorted[Math.ceil(sorted.length * percentage) - 1];
}

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function driveTransaction(
  sql: ReturnType<typeof postgres>,
  fixture: Fixture,
  runID: string,
  sequence: number,
  payloadBytes: number,
): Promise<void> {
  const id = `change-log-traffic-${runID}-${sequence}`;
  const prefix = `${runID}:${sequence}:`;
  const description =
    prefix + 'x'.repeat(Math.max(0, payloadBytes - prefix.length));

  await sql.begin(async tx => {
    await tx`
      INSERT INTO issue
        (id, title, open, "projectID", "creatorID", description, visibility)
      VALUES
        (${id}, ${`Change-log traffic ${sequence}`}, true,
         ${fixture.projectID}, ${fixture.creatorID}, ${description}, 'public')`;
    await tx`
      UPDATE issue
         SET title = ${`Updated change-log traffic ${sequence}`}, open = false
       WHERE id = ${id}`;
    await tx`DELETE FROM issue WHERE id = ${id}`;
  });
}

async function runStage(
  sql: ReturnType<typeof postgres>,
  fixture: Fixture,
  runID: string,
  firstSequence: number,
  rate: number,
  durationSeconds: number,
  concurrency: number,
  payloadBytes: number,
): Promise<StageResult> {
  const transactionCount = Math.max(1, Math.round(rate * durationSeconds));
  const active = new Set<Promise<void>>();
  const latencies: number[] = [];
  const errors: unknown[] = [];
  const startedAt = performance.now();

  for (let i = 0; i < transactionCount; i++) {
    const targetStart = startedAt + (i * 1000) / rate;
    const delay = targetStart - performance.now();
    if (delay > 0) {
      await sleep(delay);
    }
    while (active.size >= concurrency) {
      await Promise.race(active);
    }

    const transactionStartedAt = performance.now();
    const transaction = driveTransaction(
      sql,
      fixture,
      runID,
      firstSequence + i,
      payloadBytes,
    ).then(
      () => {
        latencies.push(performance.now() - transactionStartedAt);
      },
      error => {
        errors.push(error);
      },
    );
    active.add(transaction);
    void transaction.finally(() => active.delete(transaction));
  }

  await Promise.all(active);
  const elapsedSeconds = (performance.now() - startedAt) / 1000;
  latencies.sort((a, b) => a - b);
  const completed = latencies.length;

  // A stage that saw any failure aborts the run rather than reporting a
  // partial result: a rate that could not be sustained makes the latencies
  // below meaningless.
  if (errors.length > 0) {
    const first = errors[0];
    throw new Error(
      `${errors.length} transaction(s) failed. First error: ${
        first instanceof Error ? first.message : String(first)
      }`,
    );
  }

  return {
    targetTransactionsPerSecond: rate,
    durationSeconds,
    transactions: completed,
    mutations: completed * 3,
    elapsedSeconds,
    actualTransactionsPerSecond: completed / elapsedSeconds,
    latencyMs: {
      p50: percentile(latencies, 0.5),
      p95: percentile(latencies, 0.95),
      p99: percentile(latencies, 0.99),
      max: latencies.at(-1) ?? 0,
    },
  };
}

function printStage(result: StageResult): void {
  const {latencyMs} = result;
  console.log(
    [
      `target=${result.targetTransactionsPerSecond} tx/s`,
      `completed=${result.transactions}`,
      `mutations=${result.mutations}`,
      `actual=${result.actualTransactionsPerSecond.toFixed(1)} tx/s`,
      `p50=${latencyMs.p50.toFixed(1)}ms`,
      `p95=${latencyMs.p95.toFixed(1)}ms`,
      `p99=${latencyMs.p99.toFixed(1)}ms`,
      `max=${latencyMs.max.toFixed(1)}ms`,
    ].join('  '),
  );
}

async function main(): Promise<void> {
  const {values} = parseArgs({
    options: {
      'rates': {type: 'string', default: '5,25,100'},
      'duration-seconds': {type: 'string', default: '5'},
      'repeat': {type: 'string', default: '1'},
      'concurrency': {type: 'string', default: '32'},
      'payload-bytes': {type: 'string', default: '256'},
      'json': {type: 'boolean', default: false},
      'help': {type: 'boolean', default: false},
    },
    strict: true,
  });

  if (values.help) {
    console.log(USAGE.trim());
    return;
  }

  const rates = values.rates
    .split(',')
    .map((value, index) =>
      numberInRange(`rates[${index}]`, value.trim(), 0.1, 10_000),
    );
  const durationSeconds = numberInRange(
    'duration-seconds',
    values['duration-seconds'],
    0.1,
    3600,
  );
  const repeat = integerInRange('repeat', values.repeat, 1, 10_000);
  const concurrency = integerInRange('concurrency', values.concurrency, 1, 512);
  const payloadBytes = integerInRange(
    'payload-bytes',
    values['payload-bytes'],
    0,
    10_000,
  );
  const databaseURL = process.env.ZERO_UPSTREAM_DB;
  if (!databaseURL) {
    throw new Error('ZERO_UPSTREAM_DB is required');
  }

  const sql = postgres(databaseURL, {
    max: concurrency + 2,
    connect_timeout: 10,
    idle_timeout: 5,
  });
  try {
    const [fixture] = await sql<Fixture[]>`
      SELECT u.id AS "creatorID", p.id AS "projectID"
        FROM public."user" u
        CROSS JOIN public.project p
       ORDER BY u.id, p.id
       LIMIT 1`;
    if (!fixture) {
      throw new Error(
        'zbugs needs at least one user and project. Run db-seed first.',
      );
    }

    const runID = `${Date.now().toString(36)}-${process.pid}`;
    const results: StageResult[] = [];
    let sequence = 0;
    for (let repetition = 0; repetition < repeat; repetition++) {
      for (const rate of rates) {
        const result = await runStage(
          sql,
          fixture,
          runID,
          sequence,
          rate,
          durationSeconds,
          concurrency,
          payloadBytes,
        );
        results.push(result);
        sequence += Math.max(1, Math.round(rate * durationSeconds));
        if (!values.json) {
          printStage(result);
        }
      }
    }

    if (values.json) {
      console.log(JSON.stringify({runID, results}, null, 2));
    }
  } finally {
    await sql.end();
  }
}

main().catch(error => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
