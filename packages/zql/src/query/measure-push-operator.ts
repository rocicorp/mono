import type {Change} from '../ivm/change.ts';
import type {Node} from '../ivm/data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from '../ivm/operator.ts';
import type {SourceSchema} from '../ivm/schema.ts';
import type {Stream} from '../ivm/stream.ts';
import type {MetricsDelegate} from './metrics-delegate.ts';

type MetricName = 'query-update-client' | 'query-update-server';

const DEFAULT_METRICS_SAMPLE_RATE = 1;
const DISABLED_SAMPLE_EVERY = 0;
const ALWAYS_SAMPLE_EVERY = 1;

function resolveSampleEvery(metricsDelegate: MetricsDelegate): number {
  const delegate =
    metricsDelegate !== null && typeof metricsDelegate === 'object'
      ? (metricsDelegate as Record<string, unknown>)
      : undefined;

  if (delegate?.disableMetrics === true) {
    return DISABLED_SAMPLE_EVERY;
  }

  const rawRate = delegate?.metricsSampleRate;
  const sampleRate =
    typeof rawRate === 'number' && Number.isFinite(rawRate)
      ? Math.max(0, Math.min(1, rawRate))
      : DEFAULT_METRICS_SAMPLE_RATE;

  if (sampleRate <= 0) {
    return DISABLED_SAMPLE_EVERY;
  }
  if (sampleRate >= 1) {
    return ALWAYS_SAMPLE_EVERY;
  }
  return Math.max(2, Math.round(1 / sampleRate));
}

export class MeasurePushOperator implements Operator {
  readonly #input: Input;
  readonly #queryID: string;
  readonly #metricsDelegate: MetricsDelegate;

  #output: Output = throwOutput;
  readonly #metricName: MetricName;
  readonly #sampleEvery: number;
  #sampleCountdown: number;

  constructor(
    input: Input,
    queryID: string,
    metricsDelegate: MetricsDelegate,
    metricName: MetricName,
  ) {
    this.#input = input;
    this.#queryID = queryID;
    this.#metricsDelegate = metricsDelegate;
    this.#metricName = metricName;
    this.#sampleEvery = resolveSampleEvery(metricsDelegate);
    this.#sampleCountdown =
      this.#sampleEvery > ALWAYS_SAMPLE_EVERY
        ? this.#sampleEvery
        : ALWAYS_SAMPLE_EVERY;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  fetch(req: FetchRequest): Stream<Node | 'yield'> {
    return this.#input.fetch(req);
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  destroy(): void {
    this.#input.destroy();
  }

  *push(change: Change): Stream<'yield'> {
    const sampleEvery = this.#sampleEvery;

    if (sampleEvery === DISABLED_SAMPLE_EVERY) {
      yield* this.#output.push(change, this);
      return;
    }

    if (sampleEvery > ALWAYS_SAMPLE_EVERY) {
      this.#sampleCountdown -= 1;
      if (this.#sampleCountdown > 0) {
        yield* this.#output.push(change, this);
        return;
      }
      this.#sampleCountdown = sampleEvery;
    }

    const startTime = performance.now();
    yield* this.#output.push(change, this);
    this.#metricsDelegate.addMetric(
      this.#metricName,
      performance.now() - startTime,
      this.#queryID,
    );
  }
}
