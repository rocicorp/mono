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

  *push(change: Change): Stream<'yield'> {
    const startTime = performance.now();
    yield* this.#output.push(change, this);
    this.#metricsDelegate.addMetric(
      this.#metricName,
      performance.now() - startTime,
      this.#queryID,
    );
  }
}
