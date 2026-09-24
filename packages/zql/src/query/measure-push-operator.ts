import type {Change} from '../ivm/change.ts';
import type {Node} from '../ivm/data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
} from '../ivm/operator.ts';
import type {SourceSchema} from '../ivm/schema.ts';
import type {Stream} from '../ivm/stream.ts';
import type {MetricsDelegate} from './metrics-delegate.ts';

type MetricName = 'query-update-client' | 'query-update-server';

export class MeasurePushOperator implements Operator {
  readonly #input: Input;
  readonly #queryID: string;
  readonly #metricsDelegate: MetricsDelegate;

  #output: Output = throwOutput;
  readonly #metricName: MetricName;

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

  push(change: Change): Stream<'yield'> {
    return this.#measure(() => this.#output.push(change, this));
  }

  *reconcile(_pusher: InputBase): Stream<'yield'> {
    const reconcile = this.#output.reconcile?.bind(this.#output);
    if (reconcile) {
      yield* this.#measure(() => reconcile(this));
    }
  }

  /**
   * Runs the stream returned by `run` and records the time spent running it.
   *
   * The time the stream spends suspended at a `yield` is not counted: the
   * caller yields the thread there, and whatever else runs in the meantime
   * is not this query's work.
   *
   * The time is recorded even if the stream throws or is abandoned, so that
   * a push aborted for taking too long is accounted to its query.
   */
  *#measure(run: () => Stream<'yield'>): Stream<'yield'> {
    let elapsed = 0;
    let start = performance.now();
    let running = true;
    try {
      for (const result of run()) {
        elapsed += performance.now() - start;
        running = false;
        yield result;
        running = true;
        start = performance.now();
      }
    } finally {
      if (running) {
        elapsed += performance.now() - start;
      }
      this.#metricsDelegate.addMetric(this.#metricName, elapsed, this.#queryID);
    }
  }
}
