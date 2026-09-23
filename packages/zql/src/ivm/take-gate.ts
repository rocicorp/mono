import {assert} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import {constraintMatchesRow, type Constraint} from './constraint.ts';
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
 * The bound that a TakeGate caps one parent fetch of a join's child push at.
 * `undefined` means the fetch is not capped.
 */
export type ParentFetchBound = {
  readonly constraint: Constraint;
  readonly bound: Row | undefined;
};

/**
 * Reads the bounds for the parent fetches of a child push before the join
 * starts them. TakeGate reads its bound once, when a fetch starts, so the set
 * of parents that get the push stays fixed even when Take moves its bound
 * during the push. Split-push overlays must use these bounds, not the bound
 * Take reports later in the push.
 */
export function readParentFetchBounds(
  boundProvider: TakeBoundProvider,
  constraints: readonly Constraint[],
): ParentFetchBound[] {
  return constraints.map(constraint => ({
    constraint,
    bound: boundProvider.getBound(constraint),
  }));
}

/**
 * Returns true if one of the parent fetches of the push yields `row`: `row`
 * matches the fetch's constraint and is not after the fetch's bound.
 */
export function isInParentFetch(
  bounds: readonly ParentFetchBound[],
  row: Row,
  compareRows: Comparator,
): boolean {
  for (const {constraint, bound} of bounds) {
    if (constraintMatchesRow(constraint, row)) {
      return bound === undefined || compareRows(row, bound) <= 0;
    }
  }
  return false;
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

  *reconcile(_pusher: InputBase): Stream<'yield'> {
    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
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
