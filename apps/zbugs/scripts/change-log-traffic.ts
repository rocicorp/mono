/* oxlint-disable no-console */

import '../../../packages/shared/src/dotenv.ts';

import {performance} from 'node:perf_hooks';
import {argv} from 'node:process';
import {fileURLToPath} from 'node:url';
import {parseArgs} from 'node:util';
import postgres from 'postgres';

type Fixture = {
  creatorID: string;
  projectID: string;
};

/**
 * The per-transaction write shapes.
 *
 * `churn` is the original driver: insert, update and delete the same row in
 * one transaction, which commits three row mutations and leaves nothing
 * behind. Useful stress, but a *history* that leaves no residue makes the
 * replica-vs-upstream oracle vacuous -- the end state after a million
 * transactions is byte-identical to the state before them, so the comparison
 * would pass with the change log disconnected entirely.
 *
 * The other three shapes make the final state a function of the whole
 * history. A monotonically growing table also keeps the burst phase honest:
 * the backup grows, so restores in later phases move real bytes.
 */
export type TrafficShape = 'insert' | 'update' | 'delete' | 'churn';

/** The residue mix, as a 10-slot wheel indexed by transaction sequence. */
const RESIDUE_WHEEL: readonly TrafficShape[] = [
  'insert',
  'update',
  'insert',
  'update',
  'insert',
  'update',
  'insert',
  'update',
  'delete',
  'churn',
];

const PURE_CHURN_WHEEL: readonly TrafficShape[] = ['churn'];

export type TrafficStage = {
  /** Transactions per second. Zero means an idle stage: no writes at all. */
  readonly rate: number;
  readonly durationSeconds: number;
  readonly concurrency?: number | undefined;
  readonly payloadBytes?: number | undefined;
  /** Defaults to true. False restores the original pure-churn driver. */
  readonly residue?: boolean | undefined;
  /** Reported verbatim; the orchestrator uses it to name a soak phase. */
  readonly label?: string | undefined;
};

export type StageResult = {
  label: string | undefined;
  targetTransactionsPerSecond: number;
  durationSeconds: number;
  transactions: number;
  mutations: number;
  elapsedSeconds: number;
  actualTransactionsPerSecond: number;
  shapes: Record<TrafficShape, number>;
  /** Rows this run has inserted and kept, after the stage. */
  residueRows: number;
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
  --rates <list>               Comma-separated transaction rates. A rate of 0
                               is an idle stage. Default: 5,25,100
  --duration-seconds <number>  Duration of each stage. Default: 5
  --repeat <number>            Number of times to run all stages. Default: 1
  --concurrency <number>       Maximum in-flight transactions. Default: 32
  --payload-bytes <number>     Approximate issue description size. Default: 256
  --no-residue                 Use the original pure-churn shape, which leaves
                               no rows behind. Off by default: the residue mix
                               is what makes a replica-vs-upstream comparison
                               depend on the history.
  --keep                       Do not delete this run's residue rows on exit.
  --json                       Print results as JSON
  --help                       Show this help

By default each transaction is one of: insert-and-keep (40%), update a kept
row (40%), delete a kept row (10%), or the original insert+update+delete churn
(10%).
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

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export type TrafficDriverOptions = {
  readonly concurrency?: number | undefined;
  readonly runID?: string | undefined;
};

/**
 * Stages of PostgreSQL traffic against the zbugs `issue` table.
 *
 * Exported so that the soak orchestrator can interleave chaos actions between
 * (and during) stages in-process; {@link main} is a thin wrapper over the
 * same driver.
 */
export class TrafficDriver {
  readonly #sql: ReturnType<typeof postgres>;
  readonly #fixture: Fixture;
  readonly #runID: string;
  readonly #defaultConcurrency: number;
  /** Rows inserted and kept, i.e. the residue update/delete draw from. */
  readonly #kept: string[] = [];
  #sequence = 0;

  private constructor(
    sql: ReturnType<typeof postgres>,
    fixture: Fixture,
    runID: string,
    defaultConcurrency: number,
  ) {
    this.#sql = sql;
    this.#fixture = fixture;
    this.#runID = runID;
    this.#defaultConcurrency = defaultConcurrency;
  }

  static async connect(
    databaseURL: string,
    opts: TrafficDriverOptions = {},
  ): Promise<TrafficDriver> {
    const concurrency = opts.concurrency ?? 32;
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
      const runID = opts.runID ?? `${Date.now().toString(36)}-${process.pid}`;
      return new TrafficDriver(sql, fixture, runID, concurrency);
    } catch (e) {
      await sql.end();
      throw e;
    }
  }

  get runID(): string {
    return this.#runID;
  }

  /** Rows this run has inserted and not deleted. */
  get residueRows(): number {
    return this.#kept.length;
  }

  async runStage(stage: TrafficStage): Promise<StageResult> {
    const {
      rate,
      durationSeconds,
      concurrency = this.#defaultConcurrency,
      payloadBytes = 256,
      residue = true,
      label,
    } = stage;
    const shapes: Record<TrafficShape, number> = {
      insert: 0,
      update: 0,
      delete: 0,
      churn: 0,
    };

    // An idle stage. `--rates` cannot express this (its floor is 0.1/s), and
    // idle is what drains the purger to its floor and pauses the vfs poller,
    // so it is a first-class stage rather than a very low rate.
    if (rate <= 0) {
      const startedAt = performance.now();
      await sleep(durationSeconds * 1000);
      return {
        label,
        targetTransactionsPerSecond: 0,
        durationSeconds,
        transactions: 0,
        mutations: 0,
        elapsedSeconds: (performance.now() - startedAt) / 1000,
        actualTransactionsPerSecond: 0,
        shapes,
        residueRows: this.#kept.length,
        latencyMs: {p50: 0, p95: 0, p99: 0, max: 0},
      };
    }

    const wheel = residue ? RESIDUE_WHEEL : PURE_CHURN_WHEEL;
    const transactionCount = Math.max(1, Math.round(rate * durationSeconds));
    const active = new Set<Promise<void>>();
    const latencies: number[] = [];
    const errors: unknown[] = [];
    let mutations = 0;
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

      const sequence = this.#sequence++;
      // Reserve the shape and its target row synchronously, before the
      // transaction is dispatched: `delete` removes its row from the pool
      // here so that no concurrent transaction can target it too.
      const work = this.#reserve(wheel[sequence % wheel.length], sequence);
      shapes[work.shape]++;

      const transactionStartedAt = performance.now();
      const transaction = this.#drive(work, payloadBytes).then(
        () => {
          latencies.push(performance.now() - transactionStartedAt);
          mutations += work.mutations;
          if (work.shape === 'insert') {
            this.#kept.push(work.id);
          }
        },
        error => {
          errors.push(error);
          // A failed delete never happened; put the row back so the final
          // cleanup still accounts for it.
          if (work.shape === 'delete') {
            this.#kept.push(work.id);
          }
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
      label,
      targetTransactionsPerSecond: rate,
      durationSeconds,
      transactions: completed,
      mutations,
      elapsedSeconds,
      actualTransactionsPerSecond: completed / elapsedSeconds,
      shapes,
      residueRows: this.#kept.length,
      latencyMs: {
        p50: percentile(latencies, 0.5),
        p95: percentile(latencies, 0.95),
        p99: percentile(latencies, 0.99),
        max: latencies.at(-1) ?? 0,
      },
    };
  }

  /** Deletes every row this run inserted and kept. */
  async deleteResidue(): Promise<number> {
    if (this.#kept.length === 0) {
      return 0;
    }
    const ids = this.#kept.splice(0, this.#kept.length);
    for (let i = 0; i < ids.length; i += 500) {
      const batch = ids.slice(i, i + 500);
      await this.#sql`DELETE FROM issue WHERE id IN ${this.#sql(batch)}`;
    }
    return ids.length;
  }

  close(): Promise<void> {
    return this.#sql.end();
  }

  #reserve(
    requested: TrafficShape,
    sequence: number,
  ): {shape: TrafficShape; id: string; mutations: number} {
    let shape = requested;
    // The pool is empty at the start of a run, and after a delete-heavy
    // stretch. Insert instead: it is the shape that refills the pool.
    if ((shape === 'update' || shape === 'delete') && this.#kept.length === 0) {
      shape = 'insert';
    }
    switch (shape) {
      case 'update': {
        const id = this.#kept[sequence % this.#kept.length];
        return {shape, id, mutations: 1};
      }
      case 'delete': {
        const index = sequence % this.#kept.length;
        const [id] = this.#kept.splice(index, 1);
        return {shape, id, mutations: 1};
      }
      case 'churn':
        return {shape, id: this.#id(sequence), mutations: 3};
      case 'insert':
        return {shape, id: this.#id(sequence), mutations: 1};
    }
  }

  #id(sequence: number): string {
    return `change-log-traffic-${this.#runID}-${sequence}`;
  }

  #drive(
    work: {shape: TrafficShape; id: string},
    payloadBytes: number,
  ): Promise<unknown> {
    const sql = this.#sql;
    const {id, shape} = work;
    const prefix = `${this.#runID}:${id}:`;
    const description =
      prefix + 'x'.repeat(Math.max(0, payloadBytes - prefix.length));

    switch (shape) {
      case 'insert':
        return sql.begin(tx => [
          tx`
            INSERT INTO issue
              (id, title, open, "projectID", "creatorID", description, visibility)
            VALUES
              (${id}, ${`Change-log traffic ${id}`}, true,
               ${this.#fixture.projectID}, ${this.#fixture.creatorID},
               ${description}, 'public')`,
        ]);
      case 'update':
        return sql.begin(tx => [
          tx`
            UPDATE issue
               SET title = ${`Updated ${id} @ ${this.#sequence}`},
                   description = ${description},
                   open = NOT open
             WHERE id = ${id}`,
        ]);
      case 'delete':
        return sql.begin(tx => [tx`DELETE FROM issue WHERE id = ${id}`]);
      case 'churn':
        return sql.begin(async tx => {
          await tx`
            INSERT INTO issue
              (id, title, open, "projectID", "creatorID", description, visibility)
            VALUES
              (${id}, ${`Change-log traffic ${id}`}, true,
               ${this.#fixture.projectID}, ${this.#fixture.creatorID},
               ${description}, 'public')`;
          await tx`
            UPDATE issue
               SET title = ${`Updated change-log traffic ${id}`}, open = false
             WHERE id = ${id}`;
          await tx`DELETE FROM issue WHERE id = ${id}`;
        });
    }
  }
}

function printStage(result: StageResult): void {
  const {latencyMs, shapes} = result;
  if (result.targetTransactionsPerSecond === 0) {
    console.log(
      `quiet  duration=${result.durationSeconds}s  residue=${result.residueRows}`,
    );
    return;
  }
  console.log(
    [
      `target=${result.targetTransactionsPerSecond} tx/s`,
      `completed=${result.transactions}`,
      `mutations=${result.mutations}`,
      `actual=${result.actualTransactionsPerSecond.toFixed(1)} tx/s`,
      `shapes=i${shapes.insert}/u${shapes.update}/d${shapes.delete}/c${shapes.churn}`,
      `residue=${result.residueRows}`,
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
      'no-residue': {type: 'boolean', default: false},
      'keep': {type: 'boolean', default: false},
      'json': {type: 'boolean', default: false},
      'help': {type: 'boolean', default: false},
    },
    strict: true,
  });

  if (values.help) {
    console.log(USAGE.trim());
    return;
  }

  const rates = values.rates.split(',').map((value, index) => {
    const trimmed = value.trim();
    // 0 is the idle stage; anything else keeps the original 0.1 floor, below
    // which the inter-transaction sleep stops being meaningful.
    return trimmed === '0'
      ? 0
      : numberInRange(`rates[${index}]`, trimmed, 0.1, 10_000);
  });
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

  const driver = await TrafficDriver.connect(databaseURL, {concurrency});
  try {
    const results: StageResult[] = [];
    for (let repetition = 0; repetition < repeat; repetition++) {
      for (const rate of rates) {
        const result = await driver.runStage({
          rate,
          durationSeconds,
          concurrency,
          payloadBytes,
          residue: !values['no-residue'],
        });
        results.push(result);
        if (!values.json) {
          printStage(result);
        }
      }
    }

    let deleted = 0;
    if (!values.keep) {
      deleted = await driver.deleteResidue();
    }

    if (values.json) {
      console.log(
        JSON.stringify({runID: driver.runID, results, deleted}, null, 2),
      );
    } else if (deleted > 0) {
      console.log(`deleted ${deleted} residue row(s)`);
    }
  } finally {
    await driver.close();
  }
}

// Run the CLI only when this file is the entry point, so that importing
// {@link TrafficDriver} from the soak orchestrator does not start a run.
if (argv[1] && fileURLToPath(import.meta.url) === argv[1]) {
  main().catch(error => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
