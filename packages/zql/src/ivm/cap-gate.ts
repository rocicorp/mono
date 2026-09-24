import {assert} from '../../../shared/src/asserts.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {Cap} from './cap.ts';
import {deserializePKToConstraint} from './cap.ts';
import type {Change} from './change.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import {constraintContainsPartitionKey} from './take.ts';

/**
 * CapGate bounds upstream parent fetches using the active state from a
 * downstream Cap.
 *
 * In nested EXISTS subqueries, innermost child changes cause joins to fetch
 * intermediate parent rows. Without CapGate, this fetch scans all matching
 * parent rows (which may be thousands) even if downstream Cap only needs
 * at most `limit` rows (typically 1 for EXISTS). CapGate intercepts this fetch:
 * - If Cap is already at capacity for this partition (size >= limit), Cap only
 *   tracks rows in `capState.pks`. Any other row is guaranteed to be dropped
 *   downstream by Cap. CapGate performs point lookups for only the tracked PKs.
 * - If Cap has deficit or no state yet, CapGate caps the fetch at `needed` rows.
 */
export class CapGate implements Operator {
  readonly #input: Input;
  readonly #primaryKey: PrimaryKey;
  #output: Output = throwOutput;
  #cap: Cap | undefined;
  #openDepth = 0;

  constructor(input: Input) {
    input.setOutput(this);
    this.#input = input;
    this.#primaryKey = input.getSchema().primaryKey;
  }

  setCap(cap: Cap): void {
    this.#cap = cap;
  }

  open(): void {
    this.#openDepth++;
  }

  close(): void {
    assert(this.#openDepth > 0, 'CapGate.close called without matching open');
    this.#openDepth--;
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
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
    if (
      this.#openDepth > 0 ||
      req.reverse ||
      !this.#cap ||
      (this.#cap.partitionKey &&
        !constraintContainsPartitionKey(
          req.constraint,
          this.#cap.partitionKey,
        )) ||
      (req.constraint &&
        this.#primaryKey.every(
          k =>
            req.constraint &&
            k in req.constraint &&
            req.constraint[k] !== undefined,
        ))
    ) {
      yield* this.#input.fetch(req);
      return;
    }

    const capState = this.#cap.getCapState(req.constraint);
    const limit = this.#cap.limit;

    // Case 1: Cap is at capacity (size >= limit).
    // Downstream Cap will drop ANY row whose PK is not already in capState.pks.
    // Fetch only the tracked PKs directly to avoid scanning the entire partition!
    if (capState && capState.size >= limit) {
      for (const pk of capState.pks) {
        const pkConstraint = deserializePKToConstraint(pk, this.#primaryKey);
        const constraint = req.constraint
          ? {...req.constraint, ...pkConstraint}
          : pkConstraint;
        for (const node of this.#input.fetch({
          ...req,
          constraint,
        })) {
          if (node === 'yield') {
            yield node;
            continue;
          }
          yield node;
        }
      }
      return;
    }

    // Case 2: Cap is not full (size < limit or no state yet).
    // Yield at most (limit - currentSize) rows from the input fetch.
    const needed = limit - (capState?.size ?? 0);
    if (needed <= 0) {
      return;
    }
    let count = 0;
    for (const node of this.#input.fetch(req)) {
      if (node === 'yield') {
        yield node;
        continue;
      }
      yield node;
      count++;
      if (count >= needed) {
        return;
      }
    }
  }
}
