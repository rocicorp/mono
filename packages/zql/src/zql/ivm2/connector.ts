import {assert} from 'shared/src/asserts.js';
import {Change} from './change.js';
import {
  FetchRequest,
  HydrateRequest,
  Input,
  Operator,
  Output,
} from './operator.js';

/**
 * Connector is a no-op Operator that just forwards messages from input to
 * output and reverse. It exists for cases where an operator can have multiple
 * inputs or outputs. We need to be able to tell the input/outputs apart by
 * identity, so we wrap them in different Connector instances.
 */
export class Connector implements Operator {
  #input: Input;
  #output: Output | undefined;

  constructor(input: Input) {
    this.#input = input;
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  getSchema(_: Output) {
    return this.#input.getSchema(this);
  }

  hydrate(req: HydrateRequest, _: Output) {
    return this.#input.hydrate(req, this);
  }

  fetch(req: FetchRequest, _: Output) {
    return this.#input.fetch(req, this);
  }

  dehydrate(req: HydrateRequest, _: Output) {
    return this.#input.dehydrate(req, this);
  }

  push(change: Change, _: Operator) {
    assert(this.#output, 'Output not set');
    this.#output.push(change, this);
  }
}
