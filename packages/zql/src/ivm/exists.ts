import {assert, unreachable} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {Change} from './change.js';
import {normalizeUndefined, type Node, type NormalizedValue} from './data.js';
import type {
  FetchRequest,
  Input,
  Operator,
  Output,
  Storage,
} from './operator.js';
import type {SourceSchema} from './schema.js';
import {first, type Stream} from './stream.js';

interface ExistsStorage {
  get(key: string): number | undefined;
  set(key: string, value: number): void;
  del(key: string): void;
}

/**
 * The Exists operator filters data based on whether or not a relationship is
 * non-empty.
 */
export class Exists implements Operator {
  readonly #input: Input;
  readonly #relationshipName: string;
  readonly #storage: ExistsStorage;
  readonly #not: boolean;

  #output: Output | undefined;

  constructor(
    input: Input,
    storage: Storage,
    relationshipName: string,
    type: 'EXISTS' | 'NOT EXISTS',
  ) {
    this.#input = input;
    this.#relationshipName = relationshipName;
    this.#input.setOutput(this);
    this.#storage = storage as ExistsStorage;
    assert(this.#input.getSchema().relationships[relationshipName]);
    this.#not = type === 'NOT EXISTS';
  }

  setOutput(output: Output) {
    this.#output = output;
  }

  destroy(): void {
    this.#input.destroy();
  }

  getSchema(): SourceSchema {
    return this.#input.getSchema();
  }

  fetch(req: FetchRequest) {
    return this.#filter(this.#input.fetch(req));
  }

  cleanup(req: FetchRequest) {
    return this.#filter(this.#input.cleanup(req));
  }

  *#filter(stream: Stream<Node>) {
    for (const node of stream) {
      const exists = this.#fetchRelationshipAndStoreSize(node) > 0;
      if (this.#not ? !exists : exists) {
        yield node;
      }
    }
  }

  push(change: Change) {
    assert(this.#output, 'Output not set');

    switch (change.type) {
      case 'add': {
        const exists = this.#fetchRelationshipAndStoreSize(change.node) > 0;
        if (this.#not ? !exists : exists) {
          this.#output.push(change);
        }
        break;
      }
      case 'remove': {
        const exists = this.#getSize(change.node.row) > 0;
        if (this.#not ? !exists : exists) {
          this.#output.push(change);
        }
        break;
      }
      case 'child':
        if (change.child.relationshipName === this.#relationshipName) {
          switch (change.child.change.type) {
            case 'add': {
              let size = this.#getSize(change.row);
              size++;
              this.#setSize(change.row, size);
              if (size === 1) {
                this.#output.push({
                  type: this.#not ? 'remove' : 'add',
                  node: must(
                    first(
                      this.#input.fetch({
                        start: {row: change.row, basis: 'at'},
                      }),
                    ),
                  ),
                });
              }
              break;
            }
            case 'remove': {
              let size = this.#getSize(change.row);
              assert(size > 0);
              size--;
              this.#setSize(change.row, size);
              if (size === 0) {
                this.#output.push({
                  type: this.#not ? 'add' : 'remove',
                  node: must(
                    first(
                      this.#input.fetch({
                        start: {row: change.row, basis: 'at'},
                      }),
                    ),
                  ),
                });
              }
              break;
            }
          }
        }
        break;
      case 'edit': {
        const exists = this.#getSize(change.row) > 0;
        if (this.#not ? !exists : exists) {
          this.#output.push(change);
        }
        break;
      }
      default:
        unreachable(change);
    }
  }

  #getSize(row: Row): number {
    return must(this.#storage.get(this.#makeSizeStorageKey(row)));
  }

  #setSize(row: Row, size: number) {
    this.#storage.set(this.#makeSizeStorageKey(row), size);
  }

  #fetchRelationshipAndStoreSize(node: Node) {
    // We fetch this node so we can consume the relationship to
    // determine its size (we can't consume the relationship of
    // the node we are going to push or return via fetch to
    // our output, because the relationships are one time use streams).
    const relationship = must(
      first(
        this.#input.fetch({
          start: {row: node.row, basis: 'at'},
        }),
      ),
    ).relationships[this.#relationshipName];
    assert(relationship);
    let size = 0;
    for (const _relatedNode of relationship) {
      size++;
    }
    this.#storage.set(this.#makeSizeStorageKey(node.row), size);
    return size;
  }

  #makeSizeStorageKey(row: Row) {
    const primaryKey: NormalizedValue[] = [];
    for (const key of this.#input.getSchema().primaryKey) {
      primaryKey.push(normalizeUndefined(row[key]));
    }
    const json = JSON.stringify(['size', [primaryKey]]);
    return json.substring(1, json.length - 1) + ',';
  }
}
