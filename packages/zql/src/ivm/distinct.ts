import {assert} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {Change} from './change.ts';
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
import type {Stream} from './stream.ts';

const DISTINCT_SET_KEY = 'distinctSet';

type DistinctState = {
  rows: Record<string, Row>;
};

interface DistinctStorage {
  get(key: typeof DISTINCT_SET_KEY): DistinctState | undefined;
  set(key: typeof DISTINCT_SET_KEY, value: DistinctState): void;
  del(key: string): void;
}

/**
 * The Distinct operator deduplicates rows based on specified keys.
 * It maintains a set of seen rows and only passes through unique ones.
 *
 * This operator is designed to work with flat joins to collapse
 * cartesian products back to single rows based on the root table's
 * primary keys.
 * 
 * Expected row format from flat joins:
 * {
 *   ...rootTableFields,  // Fields from the root table (e.g., id, name, etc.)
 *   [flatJoinAlias]: { ...joinedRow },  // Nested joined data under an alias
 *   [anotherAlias]: { ...anotherJoinedRow }  // Multiple flat joins possible
 * }
 * 
 * The distinct operation only considers the root table's primary keys
 * for deduplication, preserving the first occurrence of each unique
 * combination of primary key values.
 */
export class Distinct implements Operator {
  readonly #input: Input;
  readonly #storage: DistinctStorage;
  readonly #keys: PrimaryKey;
  #output: Output = throwOutput;

  constructor(input: Input, storage: Storage, keys: PrimaryKey) {
    assert(keys.length > 0, 'Distinct requires at least one key');
    input.setOutput(this);
    this.#input = input;
    this.#storage = storage as DistinctStorage;
    this.#keys = keys;
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  *fetch(req: FetchRequest): Stream<Node> {
    let state = this.#storage.get(DISTINCT_SET_KEY);
    if (!state) {
      state = {rows: {}};
      this.#storage.set(DISTINCT_SET_KEY, state);
    }

    const seen = new Set<string>();

    for (const node of this.#input.fetch(req)) {
      const key = this.#getKey(node.row);

      if (!seen.has(key)) {
        seen.add(key);
        if (!state.rows[key]) {
          state.rows[key] = node.row;
          yield node;
        } else {
          // Row already exists in storage from a previous fetch
          // Only yield if it matches the stored version
          const storedRow = state.rows[key];
          if (this.#rowsMatch(storedRow, node.row)) {
            yield node;
          }
        }
      }
    }

    this.#storage.set(DISTINCT_SET_KEY, state);
  }

  *cleanup(req: FetchRequest): Stream<Node> {
    this.#storage.del(DISTINCT_SET_KEY);

    const seen = new Set<string>();
    for (const node of this.#input.cleanup(req)) {
      const key = this.#getKey(node.row);
      if (!seen.has(key)) {
        seen.add(key);
        yield node;
      }
    }
  }

  push(change: Change): void {
    let state = this.#storage.get(DISTINCT_SET_KEY);
    if (!state) {
      state = {rows: {}};
    }

    if (change.type === 'add') {
      const key = this.#getKey(change.node.row);
      if (!state.rows[key]) {
        state.rows[key] = change.node.row;
        this.#storage.set(DISTINCT_SET_KEY, state);
        this.#output.push(change);
      }
    } else if (change.type === 'remove') {
      const key = this.#getKey(change.node.row);
      if (state.rows[key]) {
        delete state.rows[key];
        this.#storage.set(DISTINCT_SET_KEY, state);
        this.#output.push(change);
      }
    } else if (change.type === 'edit') {
      const oldKey = this.#getKey(change.oldNode.row);
      const newKey = this.#getKey(change.node.row);

      if (oldKey === newKey) {
        // Keys haven't changed, update the stored row
        if (state.rows[oldKey]) {
          state.rows[oldKey] = change.node.row;
          this.#storage.set(DISTINCT_SET_KEY, state);
          this.#output.push(change);
        }
      } else {
        // Keys changed - handle as remove + add
        if (state.rows[oldKey]) {
          delete state.rows[oldKey];
          this.#output.push({
            type: 'remove',
            node: change.oldNode,
          });
        }
        if (!state.rows[newKey]) {
          state.rows[newKey] = change.node.row;
          this.#storage.set(DISTINCT_SET_KEY, state);
          this.#output.push({
            type: 'add',
            node: change.node,
          });
        }
      }
    } else if (change.type === 'child') {
      // Pass through child changes if the parent row is in our set
      const key = this.#getKey(change.node.row);
      if (state.rows[key]) {
        this.#output.push(change);
      }
    }
  }

  destroy(): void {
    this.#input.destroy();
  }

  #getKey(row: Row): string {
    const keyValues = this.#keys.map(k => row[k]);
    return JSON.stringify(keyValues);
  }

  #rowsMatch(row1: Row, row2: Row): boolean {
    return this.#keys.every(k => row1[k] === row2[k]);
  }
}
