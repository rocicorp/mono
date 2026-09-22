import {assert} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import type {Constraint} from './constraint.ts';
import type {Comparator, Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

export interface TakeBoundProvider {
  getBound(constraint?: Constraint): Row | undefined;
}

/**
 * TakeGate bounds upstream parent fetches using the active bound from a
 * downstream Take operator.
 *
 * When a child table changes, joins fetch matching parent rows. Without
 * TakeGate, this fetch scans all matching parent rows (which may be thousands)
 * even if downstream Take only needs a few. TakeGate intercepts this fetch and
 * stops the upstream stream once rows exceed Take's bound.
 */
export class TakeGate implements Operator, TakeBoundProvider {
  readonly #input: Input;
  readonly #comparator: Comparator;
  #boundProvider: TakeBoundProvider | undefined;
  #output: Output = throwOutput;
  #openDepth = 0;

  constructor(input: Input) {
    const {sort, compareRows} = input.getSchema();
    assert(sort !== undefined, 'TakeGate requires sorted input');
    this.#input = input;
    this.#comparator = compareRows;
    input.setOutput(this);
  }

  open(): void {
    this.#openDepth++;
  }

  close(): void {
    assert(this.#openDepth > 0, 'TakeGate.close called without matching open');
    this.#openDepth--;
  }

  isOpen(): boolean {
    return this.#openDepth > 0;
  }

  setBoundProvider(provider: TakeBoundProvider): void {
    this.#boundProvider = provider;
  }

  getBoundProvider(): TakeBoundProvider | undefined {
    return this.#boundProvider;
  }

  getBound(constraint?: Constraint): Row | undefined {
    if (this.#openDepth > 0) {
      return undefined;
    }
    return this.#boundProvider?.getBound(constraint);
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  *push(change: Change, _pusher: InputBase): Stream<'yield'> {
    yield* this.#output.push(change, this);
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    if (this.#openDepth > 0 || req.reverse) {
      yield* this.#input.fetch(req);
      return;
    }

    const bound = this.#boundProvider?.getBound(req.constraint);
    if (!bound) {
      yield* this.#input.fetch(req);
      return;
    }

    for (const node of this.#input.fetch(req)) {
      if (node === 'yield') {
        yield 'yield';
        continue;
      }
      if (this.#comparator(node.row, bound) > 0) {
        return;
      }
      yield node;
    }
  }
}
