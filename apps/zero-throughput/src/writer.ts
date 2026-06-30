import type {BenchmarkConfig} from './config.ts';
import type {BenchmarkDB} from './db.ts';
import {nowMs, sleep} from './util.ts';

export type WriterStats = {
  readonly startedAtMs: number;
  readonly finishedAtMs: number;
  readonly committedRows: number;
  readonly committedTransactions: number;
  readonly highestCommittedSeq: number;
  readonly transactionLatencyMs: readonly number[];
};

type InsertRow = {
  readonly id: string;
  readonly profile: string;
  readonly shard: number;
  readonly bucket: number;
  readonly seq: number;
  readonly payload: {readonly data: string};
};

export class FixedRateWriter {
  readonly #sql: BenchmarkDB;
  readonly #config: BenchmarkConfig;
  readonly #payload: {readonly data: string};
  #highestCommittedSeq = 0;

  constructor(sql: BenchmarkDB, config: BenchmarkConfig) {
    this.#sql = sql;
    this.#config = config;
    this.#payload = {data: 'x'.repeat(config.payloadBytes)};
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

      const rows: InsertRow[] = [];
      for (let i = 0; i < this.#config.batchSize; i++) {
        const seq = nextSeq++;
        rows.push({
          id: `${this.#config.runID}-${seq}`,
          profile: this.#config.profile,
          shard: 0,
          bucket: 0,
          seq,
          payload: this.#payload,
        });
      }

      const txStart = nowMs();
      await this.#sql.begin(async tx => {
        await tx`INSERT INTO zero_throughput_event ${tx(rows)}`;
      });
      transactionLatencyMs.push(nowMs() - txStart);
      committedRows += rows.length;
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
    };
  }
}
