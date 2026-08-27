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
    const concurrency = Math.max(1, this.#config.writeConcurrency);
    const workerTxRate =
      this.#config.writeRate / this.#config.batchSize / concurrency;
    const intervalMs = workerTxRate > 0 ? (1 / workerTxRate) * 1000 : 10;

    let globalSeq = 1;
    const allocateSeqs = (count: number): number[] => {
      const start = globalSeq;
      globalSeq += count;
      return Array.from({length: count}, (_, i) => start + i);
    };

    const runWorker = async () => {
      const latencies: number[] = [];
      let workerCommittedRows = 0;
      let workerCommittedTx = 0;
      let localImpact = emptyWriteImpactTotals();
      let nextStart = startedAtMs;

      while (nowMs() < deadline) {
        const delayMs = nextStart - nowMs();
        if (delayMs > 0) {
          await sleep(delayMs);
        }
        nextStart += intervalMs;

        const seqs = allocateSeqs(this.#config.batchSize);
        const txStart = nowMs();
        const impacts: WriteImpact[] = [];
        await this.#sql.begin(async tx => {
          for (const seq of seqs) {
            impacts.push(await this.#model.writeOne(tx, seq));
          }
        });
        for (const impact of impacts) {
          localImpact = addWriteImpact(localImpact, impact);
        }
        latencies.push(nowMs() - txStart);
        workerCommittedRows += seqs.length;
        workerCommittedTx++;
        const maxSeq = seqs.at(-1);
        if (maxSeq !== undefined && maxSeq > this.#highestCommittedSeq) {
          this.#highestCommittedSeq = maxSeq;
        }
      }

      return {
        latencies,
        committedRows: workerCommittedRows,
        committedTx: workerCommittedTx,
        impact: localImpact,
      };
    };

    const workerResults = await Promise.all(
      Array.from({length: concurrency}, () => runWorker()),
    );

    const transactionLatencyMs: number[] = [];
    let committedRows = 0;
    let committedTransactions = 0;
    for (const wr of workerResults) {
      transactionLatencyMs.push(...wr.latencies);
      committedRows += wr.committedRows;
      committedTransactions += wr.committedTx;
      this.#writeImpact = {
        totalLogicalWrites:
          this.#writeImpact.totalLogicalWrites + wr.impact.totalLogicalWrites,
        activePartitionWrites:
          this.#writeImpact.activePartitionWrites +
          wr.impact.activePartitionWrites,
        zeroActiveClientGroupWrites:
          this.#writeImpact.zeroActiveClientGroupWrites +
          wr.impact.zeroActiveClientGroupWrites,
        affectedActiveClientGroupWrites:
          this.#writeImpact.affectedActiveClientGroupWrites +
          wr.impact.affectedActiveClientGroupWrites,
        visibleRowWrites:
          this.#writeImpact.visibleRowWrites + wr.impact.visibleRowWrites,
        nonVisibleRowWrites:
          this.#writeImpact.nonVisibleRowWrites + wr.impact.nonVisibleRowWrites,
      };
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
