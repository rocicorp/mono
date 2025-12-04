import {areEqual} from '../../../shared/src/arrays.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import {type Change} from './change.ts';
import {normalizeUndefined, type Node, type NormalizedValue} from './data.ts';
import {
  throwFilterOutput,
  type FilterInput,
  type FilterOperator,
  type FilterOutput,
} from './filter-operators.ts';
import type {SourceSchema} from './schema.ts';

/**
 * The Exists operator filters data based on whether or not a relationship is
 * non-empty.
 */
export class Exists implements FilterOperator {
  readonly #input: FilterInput;
  readonly #relationshipName: string;
  readonly #not: boolean;
  readonly #parentJoinKey: CompoundKey;
  readonly #noSizeReuse: boolean;
  #cache: Map<string, number>;

  #output: FilterOutput = throwFilterOutput;

  /**
   * This instance variable is `true` when this operator is processing a `push`,
   * and is used to disable reuse of cached sizes across rows with the
   * same parent join key value.
   * This is necessary because during a push relationships can be inconsistent
   * due to push communicating changes (which may change multiple Nodes) one
   * Node at a time.
   */
  #inPush = false;

  constructor(
    input: FilterInput,
    relationshipName: string,
    parentJoinKey: CompoundKey,
    type: 'EXISTS' | 'NOT EXISTS',
  ) {
    this.#input = input;
    this.#relationshipName = relationshipName;
    this.#input.setFilterOutput(this);
    this.#cache = new Map();
    assert(
      this.#input.getSchema().relationships[relationshipName],
      `Input schema missing ${relationshipName}`,
    );
    this.#not = type === 'NOT EXISTS';
    this.#parentJoinKey = parentJoinKey;

    // If the parentJoinKey is the primary key, no sense in trying to reuse.
    this.#noSizeReuse = areEqual(
      parentJoinKey,
      this.#input.getSchema().primaryKey,
    );
  }

  setFilterOutput(output: FilterOutput): void {
    this.#output = output;
  }

  beginFilter() {}

  endFilter() {
    this.#cache = new Map();
  }

  filter(node: Node, cleanup: boolean): boolean {
    let size: number | undefined;
    if (!this.#noSizeReuse) {
      const key = this.#getKeyValues(node, this.#parentJoinKey).toString();
      size = this.#cache.get(key);
      if (size === undefined) {
        size = this.#fetchSize(node);
        this.#cache.set(key, size);
      }
    }

    const result =
      this.#filter(node, size) && this.#output.filter(node, cleanup);
    return result;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  push(change: Change) {
    assert(!this.#inPush, 'Unexpected re-entrancy');
    this.#inPush = true;
    try {
      switch (change.type) {
        // add, remove and edit cannot change the size of the
        // this.#relationshipName relationship, so simply #pushWithFilter
        case 'add':
        case 'edit':
        case 'remove': {
          this.#pushWithFilter(change);
          return;
        }
        case 'child':
          // Only add and remove child changes for the
          // this.#relationshipName relationship, can change the size
          // of the this.#relationshipName relationship, for other
          // child changes simply #pushWithFilter
          if (
            change.child.relationshipName !== this.#relationshipName ||
            change.child.change.type === 'edit' ||
            change.child.change.type === 'child'
          ) {
            this.#pushWithFilter(change);
            return;
          }
          switch (change.child.change.type) {
            case 'add': {
              const size = this.#fetchSize(change.node);
              if (size === 1) {
                if (this.#not) {
                  // Since the add child change currently being processed is not
                  // pushed to output, the added child needs to be excluded from
                  // the remove being pushed to output (since the child has
                  // never been added to the output).
                  this.#output.push(
                    {
                      type: 'remove',
                      node: {
                        row: change.node.row,
                        relationships: {
                          ...change.node.relationships,
                          [this.#relationshipName]: () => [],
                        },
                      },
                    },
                    this,
                  );
                } else {
                  this.#output.push(
                    {
                      type: 'add',
                      node: change.node,
                    },
                    this,
                  );
                }
              } else {
                this.#pushWithFilter(change, size);
              }
              return;
            }
            case 'remove': {
              const size = this.#fetchSize(change.node);
              if (size === 0) {
                if (this.#not) {
                  this.#output.push(
                    {
                      type: 'add',
                      node: change.node,
                    },
                    this,
                  );
                } else {
                  // Since the remove child change currently being processed is
                  // not pushed to output, the removed child needs to be added to
                  // the remove being pushed to output.
                  this.#output.push(
                    {
                      type: 'remove',
                      node: {
                        row: change.node.row,
                        relationships: {
                          ...change.node.relationships,
                          [this.#relationshipName]: () => [
                            change.child.change.node,
                          ],
                        },
                      },
                    },
                    this,
                  );
                }
              } else {
                this.#pushWithFilter(change, size);
              }
              return;
            }
          }
          return;
        default:
          unreachable(change);
      }
    } finally {
      this.#inPush = false;
    }
  }

  /**
   * Returns whether or not the node's this.#relationshipName
   * relationship passes the exist/not exists filter condition.
   * If the optional `size` is passed it is used.
   * Otherwise, if there is a stored size for the row it is used.
   * Otherwise the size is computed by streaming the node's
   * relationship with this.#relationshipName (this computed size is also
   * stored).
   */
  #filter(node: Node, size?: number): boolean {
    const exists = (size ?? this.#fetchSize(node)) > 0;
    return this.#not ? !exists : exists;
  }

  #getKeyValues(node: Node, def: CompoundKey): NormalizedValue[] {
    const values: NormalizedValue[] = [];
    for (const key of def) {
      values.push(normalizeUndefined(node.row[key]));
    }
    return values;
  }

  /**
   * Pushes a change if this.#filter is true for its row.
   */
  #pushWithFilter(change: Change, size?: number): void {
    if (this.#filter(change.node, size)) {
      this.#output.push(change, this);
    }
  }

  #fetchSize(node: Node): number {
    const relationship = node.relationships[this.#relationshipName];
    assert(relationship);
    let size = 0;
    for (const _relatedNode of relationship()) {
      size++;
    }
    return size;
  }
}
