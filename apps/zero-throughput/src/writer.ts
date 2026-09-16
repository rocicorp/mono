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
  readonly effectiveConcurrency?: number | undefined;
};

export function effectiveWriteConcurrency(config: BenchmarkConfig): number {
  if (config.writeRate <= 0) {
    return 1;
  }
  const batchSize = Math.max(1, config.batchSize);
  const targetTxRate = config.writeRate / batchSize;
  // According to Little's Law (L = lambda * W), sustaining targetTxRate with
  // transaction latency W requires at least lambda * W concurrent transactions.
  // We provision headroom for up to 1000ms latency (1.0s) so normal Aurora PG
  // latency (~400ms) with jitter does not artificially throttle the dispatch rate.
  const rateBasedConcurrency = Math.ceil(targetTxRate * 1.0);
  // Respect user-specified writeConcurrency if higher, but clamp to 256 to
  // prevent runaway connection count if batchSize is very small.
  return Math.min(256, Math.max(config.writeConcurrency, rateBasedConcurrency));
}

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
    const runStartedAtMs = nowMs();
    if (this.#config.writeRate <= 0 || durationMs <= 0) {
      if (durationMs > 0) {
        await sleep(durationMs);
      }
      return {
        startedAtMs: runStartedAtMs,
        finishedAtMs: nowMs(),
        committedRows: 0,
        committedTransactions: 0,
        highestCommittedSeq: 0,
        transactionLatencyMs: [],
        writeImpact: this.#writeImpact,
        effectiveConcurrency: 1,
      };
    }

    const batchSize = Math.max(1, this.#config.batchSize);
    const targetTxRate = this.#config.writeRate / batchSize;
    const maxConcurrency = effectiveWriteConcurrency(this.#config);
    const deadline = runStartedAtMs + durationMs;

    let globalSeq = 1;
    const allocateSeqs = (count: number): number[] => {
      const start = globalSeq;
      globalSeq += count;
      return Array.from({length: count}, (_, i) => start + i);
    };

    const latencies: number[] = [];
    let committedRows = 0;
    let committedTransactions = 0;
    let firstCommittedAtMs: number | undefined;

    const inFlight = new Set<Promise<void>>();

    const executeBatch = async (seqs: number[]) => {
      const txStart = nowMs();
      let impacts: readonly WriteImpact[] = [];
      let attempts = 0;
      while (true) {
        try {
          await this.#sql.begin(async tx => {
            impacts = await this.#model.writeBatch(tx, seqs);
          });
          break;
        } catch (err: unknown) {
          attempts++;
          const msg = err instanceof Error ? err.message : String(err);
          const isTransient =
            msg.includes('ECONNRESET') ||
            msg.includes('ETIMEDOUT') ||
            msg.includes('Connection closed');
          if (attempts < 3 && isTransient) {
            await sleep(100);
            continue;
          }
          throw err;
        }
      }
      const txEnd = nowMs();
      latencies.push(txEnd - txStart);
      committedRows += seqs.length;
      committedTransactions++;
      if (firstCommittedAtMs === undefined) {
        firstCommittedAtMs = txEnd;
      }
      for (const impact of impacts) {
        this.#writeImpact = addWriteImpact(this.#writeImpact, impact);
      }
      const maxSeq = seqs.at(-1);
      if (maxSeq !== undefined && maxSeq > this.#highestCommittedSeq) {
        this.#highestCommittedSeq = maxSeq;
      }
    };

    let txIndex = 0;
    let firstError: unknown = null;

    try {
      while (nowMs() < deadline) {
        if (firstError) {
          break;
        }

        const targetTime = runStartedAtMs + (txIndex * 1000) / targetTxRate;
        if (targetTime >= deadline) {
          break;
        }

        const delayMs = targetTime - nowMs();
        if (delayMs > 0) {
          await sleep(delayMs);
        }

        if (firstError) {
          break;
        }

        if (inFlight.size >= maxConcurrency) {
          await Promise.race(inFlight);
        }

        if (firstError) {
          break;
        }

        const seqs = allocateSeqs(batchSize);
        let task: Promise<void>;
        task = executeBatch(seqs)
          .catch(err => {
            if (!firstError) {
              firstError = err;
            }
          })
          .finally(() => {
            inFlight.delete(task);
          });
        inFlight.add(task);
        txIndex++;
      }

      await Promise.all(inFlight);
    } catch (err) {
      await Promise.allSettled(inFlight);
      throw err;
    }

    if (firstError) {
      throw firstError;
    }

    const finishedAtMs = nowMs();
    const startedAtMs =
      firstCommittedAtMs !== undefined && firstCommittedAtMs < finishedAtMs
        ? firstCommittedAtMs
        : runStartedAtMs;

    return {
      startedAtMs,
      finishedAtMs,
      committedRows,
      committedTransactions,
      highestCommittedSeq: this.#highestCommittedSeq,
      transactionLatencyMs: latencies,
      writeImpact: this.#writeImpact,
      effectiveConcurrency: maxConcurrency,
    };
  }
}
