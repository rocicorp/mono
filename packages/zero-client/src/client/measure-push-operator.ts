import type {Change} from '../../../zql/src/ivm/change.ts';
import type {Node} from '../../../zql/src/ivm/data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from '../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../zql/src/ivm/schema.ts';
import type {Stream} from '../../../zql/src/ivm/stream.ts';
import type {MetricsDelegate} from '../../../zql/src/query/metrics-delegate.ts';

export class MeasurePushOperator implements Operator {
  readonly #input: Input;
  readonly #queryID: string;
  readonly #metricsDelegate: MetricsDelegate;

  #output: Output = throwOutput;

  constructor(input: Input, queryID: string, metricsDelegate: MetricsDelegate) {
    this.#input = input;
    this.#queryID = queryID;
    this.#metricsDelegate = metricsDelegate;
    input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  fetch(req: FetchRequest): Stream<Node> {
    return this.#input.fetch(req);
  }

  cleanup(req: FetchRequest): Stream<Node> {
    return this.#input.cleanup(req);
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  destroy(): void {
    this.#input.destroy();
  }

  push(change: Change): void {
    const startTime = performance.now();
    this.#output.push(change);
    this.#metricsDelegate.addMetric(
      'query-update-client',
      performance.now() - startTime,
      this.#queryID,
    );
  }
}
