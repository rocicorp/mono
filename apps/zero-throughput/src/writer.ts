import type {BenchmarkConfig} from './config.ts';
import type {BenchmarkDB} from './db.ts';
import {nowMs, sleep} from './util.ts';
import {
  addWriteImpact,
  createThroughputWriteModel,
  emptyWriteImpactTotals,
  type ThroughputWriteModel,
  type WriteImpact,
  type WriteImpactTotals,
} from './workload-models.ts';

export type WriterStats = {
  readonly startedAtMs: number;
  readonly finishedAtMs: number;
  readonly committedRows: number;
  readonly committedTransactions: number;
  readonly highestCommittedSeq: number;
  readonly transactionLatencyMs: readonly number[];
  readonly writeImpact: WriteImpactTotals;
};

export type WriterProgress = {
  readonly highestCommittedSeq: number;
};

export class FixedRateWriter {
  readonly #sql: BenchmarkDB;
  readonly #config: BenchmarkConfig;
  readonly #payload: string;
  readonly #model: ThroughputWriteModel;
  #highestCommittedSeq = 0;
  #writeImpact = emptyWriteImpactTotals();

  constructor(sql: BenchmarkDB, config: BenchmarkConfig) {
    this.#sql = sql;
    this.#config = config;
    this.#payload = 'x'.repeat(config.payloadBytes);
    this.#model = createThroughputWriteModel(config, this.#payload);
  }

  get highestCommittedSeq(): number {
    return this.#highestCommittedSeq;
  }

  async run(durationMs: number): Promise<WriterStats> {
    const startedAtMs = nowMs();
    const deadline = startedAtMs + durationMs;
    const transactionLatencyMs: number[] = [];
    let committedRows = 0;
    let committedTransactions = 0;
    let nextSeq = 1;
    let nextStart = startedAtMs;
    const intervalMs = (this.#config.batchSize / this.#config.writeRate) * 1000;

    while (nowMs() < deadline) {
      const delayMs = nextStart - nowMs();
      if (delayMs > 0) {
        await sleep(delayMs);
      }
      nextStart += intervalMs;

      const seqs = Array.from(
        {length: this.#config.batchSize},
        () => nextSeq++,
      );

      const txStart = nowMs();
      const impacts: WriteImpact[] = [];
      await this.#sql.begin(async tx => {
        for (const seq of seqs) {
          impacts.push(await this.#model.writeOne(tx, seq));
        }
      });
      for (const impact of impacts) {
        this.#writeImpact = addWriteImpact(this.#writeImpact, impact);
      }
      transactionLatencyMs.push(nowMs() - txStart);
      committedRows += seqs.length;
      committedTransactions++;
      this.#highestCommittedSeq = nextSeq - 1;
    }

    return {
      startedAtMs,
      finishedAtMs: nowMs(),
      committedRows,
      committedTransactions,
      highestCommittedSeq: this.#highestCommittedSeq,
      transactionLatencyMs,
      writeImpact: this.#writeImpact,
    };
  }
}

/**
 * Writes feed rows with one set-based INSERT per transaction. Unlike the
 * steady-state writer, this can create a large transaction without turning
 * upstream SQL round trips into the bottleneck being measured.
 */
export class MigrationWriter implements WriterProgress {
  readonly #sql: BenchmarkDB;
  readonly #config: BenchmarkConfig;
  readonly #payload: string;
  #highestCommittedSeq = 0;

  constructor(sql: BenchmarkDB, config: BenchmarkConfig) {
    this.#sql = sql;
    this.#config = config;
    this.#payload = 'x'.repeat(config.payloadBytes);
  }

  get highestCommittedSeq(): number {
    return this.#highestCommittedSeq;
  }

  async run(totalRows: number): Promise<WriterStats> {
    const startedAtMs = nowMs();
    const transactionLatencyMs: number[] = [];
    let committedRows = 0;
    let committedTransactions = 0;
    let nextSeq = 1;
    let nextStart = startedAtMs;
    const completedBatches = new Map<number, number>();

    const runWorker = async () => {
      for (;;) {
        const firstSeq = nextSeq;
        if (firstSeq > totalRows) {
          return;
        }
        const rows = Math.min(this.#config.batchSize, totalRows - firstSeq + 1);
        const lastSeq = firstSeq + rows - 1;
        nextSeq = lastSeq + 1;

        const scheduledStart = nextStart;
        nextStart += (rows / this.#config.writeRate) * 1_000;
        const delayMs = scheduledStart - nowMs();
        if (delayMs > 0) {
          await sleep(delayMs);
        }

        const txStart = nowMs();
        await this.#sql`
          INSERT INTO zero_throughput_event
            (id, profile, shard, bucket, seq, payload)
          SELECT
            ${this.#config.runID} || '-' || migrated.seq::text,
            ${this.#config.profile},
            0,
            0,
            migrated.seq,
            ${this.#sql.json({data: this.#payload})}
          FROM generate_series(${firstSeq}::bigint, ${lastSeq}::bigint)
            AS migrated(seq)
        `;
        transactionLatencyMs.push(nowMs() - txStart);
        committedRows += rows;
        committedTransactions++;
        completedBatches.set(firstSeq, lastSeq);

        for (;;) {
          const completedThrough = completedBatches.get(
            this.#highestCommittedSeq + 1,
          );
          if (completedThrough === undefined) {
            break;
          }
          completedBatches.delete(this.#highestCommittedSeq + 1);
          this.#highestCommittedSeq = completedThrough;
        }
      }
    };

    await Promise.all(
      Array.from({length: this.#config.migration.concurrency}, () =>
        runWorker(),
      ),
    );

    return {
      startedAtMs,
      finishedAtMs: nowMs(),
      committedRows,
      committedTransactions,
      highestCommittedSeq: this.#highestCommittedSeq,
      transactionLatencyMs,
      writeImpact: {
        totalLogicalWrites: committedRows,
        activePartitionWrites: committedRows,
        zeroActiveClientGroupWrites: 0,
        affectedActiveClientGroupWrites: committedRows,
        visibleRowWrites: committedRows,
        nonVisibleRowWrites: 0,
      },
    };
  }
}
