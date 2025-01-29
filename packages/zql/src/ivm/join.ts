import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {Change, ChildChange} from './change.ts';
import {valuesEqual, type Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {take, type Stream} from './stream.ts';

type Args = {
  parent: Input;
  child: Input;
  storage: Storage;
  // The order of the keys does not have to match but the length must match.
  // The nth key in parentKey corresponds to the nth key in childKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;

  // TODO: Change parentKey & childKey to a correlation

  relationshipName: string;
  hidden: boolean;
  system: System;
};
/**
 * The Join operator joins the output from two upstream inputs. Zero's join
 * is a little different from SQL's join in that we output hierarchical data,
 * not a flat table. This makes it a lot more useful for UI programming and
 * avoids duplicating tons of data like left join would.
 *
 * The Nodes output from Join have a new relationship added to them, which has
 * the name #relationshipName. The value of the relationship is a stream of
 * child nodes which are the corresponding values from the child source.
 */
export class Join implements Input {
  readonly #parent: Input;
  readonly #child: Input;
  readonly #storage: Storage;
  readonly #parentKey: CompoundKey;
  readonly #childKey: CompoundKey;
  readonly #relationshipName: string;
  readonly #schema: SourceSchema;

  #output: Output = throwOutput;

  constructor({
    parent,
    child,
    storage,
    parentKey,
    childKey,
    relationshipName,
    hidden,
    system,
  }: Args) {
    assert(parent !== child, 'Parent and child must be different operators');
    assert(
      parentKey.length === childKey.length,
      'The parentKey and childKey keys must have same length',
    );
    this.#parent = parent;
    this.#child = child;
    this.#storage = storage;
    this.#parentKey = parentKey;
    this.#childKey = childKey;
    this.#relationshipName = relationshipName;

    const parentSchema = parent.getSchema();
    const childSchema = child.getSchema();
    this.#schema = {
      ...parentSchema,
      isHidden: hidden,
      relationships: {
        ...parentSchema.relationships,
        [relationshipName]: {
          ...childSchema,
          system,
        },
      },
    };

    parent.setOutput({
      push: (change: Change) => this.#pushParent(change),
    });
    child.setOutput({
      push: (change: Change) => this.#pushChild(change),
    });
  }

  destroy(): void {
    this.#parent.destroy();
    this.#child.destroy();
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node> {
    for (const parentNode of this.#parent.fetch(req)) {
      yield this.#processParentNode(
        parentNode.row,
        parentNode.relationships,
        'fetch',
      );
    }
  }

  *cleanup(req: FetchRequest): Stream<Node> {
    for (const parentNode of this.#parent.cleanup(req)) {
      yield this.#processParentNode(
        parentNode.row,
        parentNode.relationships,
        'cleanup',
      );
    }
  }

  #pushParent(change: Change): void {
    switch (change.type) {
      case 'add':
        this.#output.push({
          type: 'add',
          node: this.#processParentNode(
            change.node.row,
            change.node.relationships,
            'fetch',
          ),
        });
        break;
      case 'remove':
        this.#output.push({
          type: 'remove',
          node: this.#processParentNode(
            change.node.row,
            change.node.relationships,
            'cleanup',
          ),
        });
        break;
      case 'child':
        this.#output.push(change);
        break;
      case 'edit': {
        // When an edit comes in we need to:
        // - If the value of the join key did not change we can forward
        //   as an edit but with relationships added
        // - Otherwise we convert to a remove and add

        if (
          rowEqualsForCompoundKey(
            change.oldNode.row,
            change.node.row,
            this.#parentKey,
          )
        ) {
          this.#output.push({
            type: 'edit',
            oldNode: this.#processParentNode(
              change.oldNode.row,
              change.oldNode.relationships,
              'cleanup',
            ),
            node: this.#processParentNode(
              change.node.row,
              change.node.relationships,
              'fetch',
            ),
          });
        } else {
          this.#pushParent({
            type: 'remove',
            node: change.oldNode,
          });
          this.#pushParent({
            type: 'add',
            node: change.node,
          });
        }

        break;
      }
      default:
        unreachable(change);
    }
  }

  #pushChild(change: Change): void {
    const pushChildChange = (childRow: Row, change: Change) => {
      const parentNodes = this.#parent.fetch({
        constraint: Object.fromEntries(
          this.#parentKey.map((key, i) => [key, childRow[this.#childKey[i]]]),
        ),
      });

      for (const parentNode of parentNodes) {
        const childChange: ChildChange = {
          type: 'child',
          node: parentNode,
          child: {
            relationshipName: this.#relationshipName,
            change,
          },
        };
        this.#output.push(childChange);
      }
    };

    switch (change.type) {
      case 'add':
      case 'remove':
        pushChildChange(change.node.row, change);
        break;
      case 'child':
        pushChildChange(change.node.row, change);
        break;
      case 'edit': {
        const childRow = change.node.row;
        const oldChildRow = change.oldNode.row;
        if (rowEqualsForCompoundKey(oldChildRow, childRow, this.#childKey)) {
          // The child row was edited in a way that does not change the relationship.
          // We can therefore just push the change down (wrapped in a child change).
          pushChildChange(childRow, change);
        } else {
          // The child row was edited in a way that changes the relationship. We
          // therefore treat this as a remove from the old row followed by an
          // add to the new row.
          pushChildChange(oldChildRow, {
            type: 'remove',
            node: change.oldNode,
          });
          pushChildChange(childRow, {
            type: 'add',
            node: change.node,
          });
        }
        break;
      }

      default:
        unreachable(change);
    }
  }

  #processParentNode(
    parentNodeRow: Row,
    parentNodeRelations: Record<string, () => Stream<Node>>,
    mode: ProcessParentMode,
  ): Node {
    let method: ProcessParentMode = mode;
    if (mode === 'cleanup') {
      this.#storage.del(
        makeStorageKey(
          this.#parentKey,
          this.#parent.getSchema().primaryKey,
          parentNodeRow,
        ),
      );
      const empty =
        [
          ...take(
            this.#storage.scan({
              prefix: makeStorageKeyPrefix(parentNodeRow, this.#parentKey),
            }),
            1,
          ),
        ].length === 0;
      method = empty ? 'cleanup' : 'fetch';
    }

    let storageUpdated = false;
    const childStream = () => {
      if (!storageUpdated) {
        storageUpdated = true;
        // Defer the work to update storage until the child stream
        // is actually accessed
        if (mode === 'fetch') {
          this.#storage.set(
            makeStorageKey(
              this.#parentKey,
              this.#parent.getSchema().primaryKey,
              parentNodeRow,
            ),
            true,
          );
        }
      }
      return this.#child[method]({
        constraint: Object.fromEntries(
          this.#childKey.map((key, i) => [
            key,
            parentNodeRow[this.#parentKey[i]],
          ]),
        ),
      });
    };

    return {
      row: parentNodeRow,
      relationships: {
        ...parentNodeRelations,
        [this.#relationshipName]: childStream,
      },
    };
  }
}

type ProcessParentMode = 'fetch' | 'cleanup';

/** Exported for testing. */
export function makeStorageKeyForValues(values: readonly Value[]): string {
  const json = JSON.stringify(['pKeySet', ...values]);
  return json.substring(1, json.length - 1) + ',';
}

/** Exported for testing. */
export function makeStorageKeyPrefix(row: Row, key: CompoundKey): string {
  return makeStorageKeyForValues(key.map(k => row[k]));
}

/** Exported for testing.
 * This storage key tracks the primary keys seen for each unique
 * value joined on. This is used to know when to cleanup a child's state.
 */
export function makeStorageKey(
  key: CompoundKey,
  primaryKey: PrimaryKey,
  row: Row,
): string {
  const values: Value[] = key.map(k => row[k]);
  for (const key of primaryKey) {
    values.push(row[key]);
  }
  return makeStorageKeyForValues(values);
}

function rowEqualsForCompoundKey(a: Row, b: Row, key: CompoundKey): boolean {
  for (let i = 0; i < key.length; i++) {
    if (!valuesEqual(a[key[i]], b[key[i]])) {
      return false;
    }
  }
  return true;
}
