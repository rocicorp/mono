import {assert} from '../../../shared/src/asserts.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import {type Change, type EditChange} from './change.ts';
import type {Constraint} from './constraint.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import {constraintMatchesPartitionKey, type PartitionKey} from './take.ts';

type CapState = {
  size: number;
  pks: string[];
};

interface CapStorage {
  get(key: string): CapState | undefined;
  set(key: string, value: CapState): void;
  del(key: string): void;
}

/**
 * The Cap operator is a count-based limiter for EXISTS subqueries that
 * does not require ordering. Unlike Take, it tracks membership by primary
 * key set rather than by a sorted bound. This means:
 *
 * - No comparator needed (no ordering requirement)
 * - No `start` or `reverse` fetch support
 * - No `#rowHiddenFromFetch` complexity
 *
 * Cap is used in EXISTS child pipelines where only the count of matching
 * rows matters, not their order. This allows SQLite to skip ORDER BY
 * entirely, enabling much faster query plans.
 *
 * Cap can count rows globally or by unique value of some partition key
 * (same as Take).
 */
export class Cap implements Operator {
  readonly #input: Input;
  readonly #storage: CapStorage;
  readonly #limit: number;
  readonly #partitionKey: PartitionKey | undefined;
  readonly #primaryKey: PrimaryKey;

  #output: Output = throwOutput;

  constructor(
    input: Input,
    storage: Storage,
    limit: number,
    partitionKey?: PartitionKey,
  ) {
    assert(limit >= 0, 'Limit must be non-negative');
    input.setOutput(this);
    this.#input = input;
    this.#storage = storage as CapStorage;
    this.#limit = limit;
    this.#partitionKey = partitionKey;
    this.#primaryKey = input.getSchema().primaryKey;
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    assert(!req.start, 'Cap does not support start');
    assert(!req.reverse, 'Cap does not support reverse');

    if (
      !this.#partitionKey ||
      (req.constraint &&
        constraintMatchesPartitionKey(req.constraint, this.#partitionKey))
    ) {
      const capStateKey = getCapStateKey(this.#partitionKey, req.constraint);
      const capState = this.#storage.get(capStateKey);
      if (!capState) {
        yield* this.#initialFetch(req);
        return;
      }
      if (capState.size === 0) {
        return;
      }
      // PK-based point lookups: fetch each tracked row by its PK directly,
      // rather than scanning the partition and filtering.
      for (const pk of capState.pks) {
        const constraint = deserializePKToConstraint(pk, this.#primaryKey);
        for (const inputNode of this.#input.fetch({constraint})) {
          if (inputNode === 'yield') {
            yield inputNode;
            continue;
          }
          yield inputNode;
        }
      }
      return;
    }
    // There is a partition key, but the fetch is not constrained or constrained
    // on a different key. This currently only happens with nested sub-queries.
    for (const inputNode of this.#input.fetch(req)) {
      if (inputNode === 'yield') {
        yield inputNode;
        continue;
      }
      const capStateKey = getCapStateKey(this.#partitionKey, inputNode.row);
      const capState = this.#storage.get(capStateKey);
      if (capState && capState.size > 0) {
        const pkSet = new Set(capState.pks);
        const pk = serializePK(inputNode.row, this.#primaryKey);
        if (pkSet.has(pk)) {
          yield inputNode;
        }
      }
    }
  }

  *#initialFetch(req: FetchRequest): Stream<Node | 'yield'> {
    assert(
      constraintMatchesPartitionKey(req.constraint, this.#partitionKey),
      'Constraint should match partition key',
    );

    if (this.#limit === 0) {
      return;
    }

    const capStateKey = getCapStateKey(this.#partitionKey, req.constraint);
    assert(
      this.#storage.get(capStateKey) === undefined,
      'Cap state should be undefined',
    );

    let size = 0;
    const pks: string[] = [];
    for (const inputNode of this.#input.fetch(req)) {
      if (inputNode === 'yield') {
        yield 'yield';
        continue;
      }
      yield inputNode;
      pks.push(serializePK(inputNode.row, this.#primaryKey));
      size++;
      if (size === this.#limit) {
        break;
      }
    }
    this.#storage.set(capStateKey, {size, pks});
  }

  *push(change: Change): Stream<'yield'> {
    if (change.type === 'edit') {
      yield* this.#pushEditChange(change);
      return;
    }

    const capStateKey = getCapStateKey(this.#partitionKey, change.node.row);
    const capState = this.#storage.get(capStateKey);
    if (!capState) {
      return;
    }

    const pk = serializePK(change.node.row, this.#primaryKey);

    if (change.type === 'add') {
      if (capState.size < this.#limit) {
        const pks = [...capState.pks, pk];
        this.#storage.set(capStateKey, {size: capState.size + 1, pks});
        yield* this.#output.push(change, this);
        return;
      }
      // Full — drop
      return;
    } else if (change.type === 'remove') {
      const pkIndex = capState.pks.indexOf(pk);
      if (pkIndex === -1) {
        // Not in our set — drop
        return;
      }
      // Remove from set
      const pks = [...capState.pks];
      pks.splice(pkIndex, 1);
      const newSize = capState.size - 1;

      // Try to refill: fetch from input with partition constraint,
      // find first row NOT in PK set
      const pkSet = new Set(pks);
      const constraint = this.#partitionKey
        ? (Object.fromEntries(
            this.#partitionKey.map(key => [key, change.node.row[key]] as const),
          ) as Constraint)
        : undefined;

      let replacement: Node | undefined;
      for (const node of this.#input.fetch({constraint})) {
        if (node === 'yield') {
          yield node;
          continue;
        }
        const nodePK = serializePK(node.row, this.#primaryKey);
        if (!pkSet.has(nodePK)) {
          replacement = node;
          break;
        }
      }

      if (replacement) {
        // Store state WITHOUT replacement during remove forward,
        // matching Take's pattern of hiding in-flight changes from re-fetches.
        this.#storage.set(capStateKey, {size: newSize, pks});
        yield* this.#output.push(change, this);
        // Now add replacement to set and forward the add.
        const replacementPK = serializePK(replacement.row, this.#primaryKey);
        pks.push(replacementPK);
        this.#storage.set(capStateKey, {size: newSize + 1, pks});
        yield* this.#output.push({type: 'add', node: replacement}, this);
      } else {
        this.#storage.set(capStateKey, {size: newSize, pks});
        yield* this.#output.push(change, this);
      }
    } else if (change.type === 'child') {
      const pkSet = new Set(capState.pks);
      if (pkSet.has(pk)) {
        yield* this.#output.push(change, this);
      }
    }
  }

  *#pushEditChange(change: EditChange): Stream<'yield'> {
    const capStateKey = getCapStateKey(this.#partitionKey, change.oldNode.row);
    const capState = this.#storage.get(capStateKey);
    if (!capState) {
      return;
    }

    const oldPK = serializePK(change.oldNode.row, this.#primaryKey);
    const pkSet = new Set(capState.pks);
    if (pkSet.has(oldPK)) {
      // Update the PK in our set if it changed
      const newPK = serializePK(change.node.row, this.#primaryKey);
      if (oldPK !== newPK) {
        const pks = capState.pks.map(p => (p === oldPK ? newPK : p));
        this.#storage.set(capStateKey, {size: capState.size, pks});
      }
      yield* this.#output.push(change, this);
    }
    // If not in our set, drop
  }

  destroy(): void {
    this.#input.destroy();
  }
}

function getCapStateKey(
  partitionKey: PartitionKey | undefined,
  rowOrConstraint: Row | Constraint | undefined,
): string {
  const partitionValues: Value[] = [];

  if (partitionKey && rowOrConstraint) {
    for (const key of partitionKey) {
      partitionValues.push(rowOrConstraint[key]);
    }
  }

  return JSON.stringify(['cap', ...partitionValues]);
}

function serializePK(row: Row, primaryKey: PrimaryKey): string {
  return JSON.stringify(primaryKey.map(k => row[k]));
}

function deserializePKToConstraint(
  pk: string,
  primaryKey: PrimaryKey,
): Constraint {
  const values = JSON.parse(pk) as Value[];
  const constraint: Record<string, Value> = {};
  for (let i = 0; i < primaryKey.length; i++) {
    constraint[primaryKey[i]] = values[i];
  }
  return constraint;
}
