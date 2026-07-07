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
