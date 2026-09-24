import {assert} from '../../../shared/src/asserts.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {makeAddChange, type Change, type EditChange} from './change.ts';
import type {Constraint} from './constraint.ts';
import type {Comparator, Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import {
  constraintMatchesPartitionKey,
  makePartitionKeyComparator,
  type PartitionKey,
} from './take.ts';

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
 * - No `#rowHiddenFromFetch` complexity (we can defer when adding to the pk set)
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
  readonly #partitionKeyComparator: Comparator | undefined;
  readonly #primaryKey: PrimaryKey;

  #output: Output = throwOutput;
  readonly #dirtyPartitions = new Map<string, Constraint | undefined>();

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
    this.#partitionKeyComparator =
      partitionKey && makePartitionKeyComparator(partitionKey);
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

    // Cap is only built for non-flipped EXISTS subqueries, whose only
    // downstream consumer is a Join that always fetches with a constraint
    // built from the correlation's childField — which is Cap's partition
    // key. So either partitionKey is undefined, or constraint matches.
    assert(
      !this.#partitionKey ||
        (req.constraint !== undefined &&
          constraintMatchesPartitionKey(req.constraint, this.#partitionKey)),
      'Cap fetch: constraint must match partition key when partitioned',
    );

    const capStateKey = getCapStateKey(this.#partitionKey, req.constraint);
    const capState = this.#storage.get(capStateKey);
    if (!capState) {
      yield* this.#initialFetch(req);
      return;
    }
    if (capState.size === 0) {
      return;
    }
    for (const pk of capState.pks) {
      const pkConstraint = deserializePKToConstraint(pk, this.#primaryKey);
      // Preserve req.constraint (the partition key) so upstream partitioned
      // operators (e.g. Take) retain their partition scope and do not throw.
      const constraint = req.constraint
        ? {...req.constraint, ...pkConstraint}
        : pkConstraint;
      for (const inputNode of this.#input.fetch({constraint})) {
        if (inputNode === 'yield') {
          yield inputNode;
          continue;
        }
        yield inputNode;
      }
    }
  }

  *#initialFetch(req: FetchRequest): Stream<Node | 'yield'> {
    if (this.#limit === 0) {
      return;
    }

    assert(
      constraintMatchesPartitionKey(req.constraint, this.#partitionKey),
      'Constraint should match partition key',
    );

    const capStateKey = getCapStateKey(this.#partitionKey, req.constraint);
    assert(
      this.#storage.get(capStateKey) === undefined,
      'Cap state should be undefined',
    );

    let size = 0;
    const pks: string[] = [];
    let downstreamEarlyReturn = true;
    let exceptionThrown = false;
    try {
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
      downstreamEarlyReturn = false;
    } catch (e) {
      exceptionThrown = true;
      throw e;
    } finally {
      if (!exceptionThrown) {
        this.#storage.set(capStateKey, {size, pks});
        // If it becomes necessary to support downstream early return, this
        // assert should be removed, and replaced with code that consumes
        // the input stream until limit is reached or the input stream is
        // exhausted so that capState is properly hydrated.
        assert(
          !downstreamEarlyReturn,
          'Unexpected early return prevented full hydration',
        );
      }
    }
  }

  *push(change: Change): Stream<'yield'> {
    if (change[ChangeIndex.TYPE] === ChangeType.EDIT) {
      yield* this.#pushEditChange(change);
      return;
    }

    const capStateKey = getCapStateKey(
      this.#partitionKey,
      change[ChangeIndex.NODE].row,
    );
    const capState = this.#storage.get(capStateKey);
    if (!capState) {
      return;
    }

    const pk = serializePK(change[ChangeIndex.NODE].row, this.#primaryKey);

    if (change[ChangeIndex.TYPE] === ChangeType.ADD) {
      if (capState.size < this.#limit) {
        const pks = [...capState.pks, pk];
        this.#storage.set(capStateKey, {size: capState.size + 1, pks});
        yield* this.#output.push(change, this);
        return;
      }
      // Full — drop
      return;
    } else if (change[ChangeIndex.TYPE] === ChangeType.REMOVE) {
      const pkIndex = capState.pks.indexOf(pk);
      if (pkIndex === -1) {
        // Not in our set — drop
        return;
      }
      // Remove from set
      const pks = [...capState.pks];
      pks.splice(pkIndex, 1);
      const newSize = capState.size - 1;

      const constraint = this.#partitionKey
        ? (Object.fromEntries(
            this.#partitionKey.map(
              key => [key, change[ChangeIndex.NODE].row[key]] as const,
            ),
          ) as Constraint)
        : undefined;

      this.#dirtyPartitions.set(capStateKey, constraint);
      this.#storage.set(capStateKey, {size: newSize, pks});
      yield* this.#output.push(change, this);
    } else if (change[ChangeIndex.TYPE] === ChangeType.CHILD) {
      const pkSet = new Set(capState.pks);
      if (pkSet.has(pk)) {
        yield* this.#output.push(change, this);
      }
    }
  }

  #pushEditChange(change: EditChange): Stream<'yield'> {
    assert(
      !this.#partitionKeyComparator ||
        this.#partitionKeyComparator(
          change[ChangeIndex.OLD_NODE].row,
          change[ChangeIndex.NODE].row,
        ) === 0,
      'Unexpected change of partition key',
    );
    const capStateKey = getCapStateKey(
      this.#partitionKey,
      change[ChangeIndex.OLD_NODE].row,
    );
    const capState = this.#storage.get(capStateKey);
    if (!capState) {
      return emptyArray;
    }

    const oldPK = serializePK(
      change[ChangeIndex.OLD_NODE].row,
      this.#primaryKey,
    );
    const pkSet = new Set(capState.pks);
    if (pkSet.has(oldPK)) {
      // Update the PK in our set if it changed
      const newPK = serializePK(change[ChangeIndex.NODE].row, this.#primaryKey);
      if (oldPK !== newPK) {
        const pks = capState.pks.map(p => (p === oldPK ? newPK : p));
        this.#storage.set(capStateKey, {size: capState.size, pks});
      }
      return this.#output.push(change, this);
    }
    // If not in our set, drop
    return emptyArray;
  }

  destroy(): void {
    this.#input.destroy();
  }

  *reconcile(_pusher: InputBase): Stream<'yield'> {
    if (this.#dirtyPartitions.size > 0) {
      const dirty = [...this.#dirtyPartitions.entries()];
      this.#dirtyPartitions.clear();

      for (const [capStateKey, constraint] of dirty) {
        const capState = this.#storage.get(capStateKey);
        if (!capState) {
          continue;
        }
        const deficit = this.#limit - capState.size;
        if (deficit <= 0) {
          continue;
        }

        const pkSet = new Set(capState.pks);
        const replacements: Node[] = [];
        for (const node of this.#input.fetch({constraint})) {
          if (node === 'yield') {
            yield node;
            continue;
          }
          const nodePK = serializePK(node.row, this.#primaryKey);
          if (!pkSet.has(nodePK)) {
            replacements.push(node);
            pkSet.add(nodePK);
            if (replacements.length === deficit) {
              break;
            }
          }
        }

        let newSize = capState.size;
        const pks = [...capState.pks];
        for (const replacement of replacements) {
          const replacementPK = serializePK(replacement.row, this.#primaryKey);
          pks.push(replacementPK);
          newSize++;
          this.#storage.set(capStateKey, {size: newSize, pks});
          yield* this.#output.push(makeAddChange(replacement), this);
        }
      }
    }

    if (this.#output.reconcile) {
      yield* this.#output.reconcile(this);
    }
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
