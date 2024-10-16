import {assert, unreachable} from '../../../../shared/src/asserts.js';
import {must} from '../../../../shared/src/must.js';
import type {Change, ChildChange} from './change.js';
import {
  normalizeUndefined,
  type Node,
  type NormalizedValue,
  type Row,
} from './data.js';
import type {FetchRequest, Input, Output, Storage} from './operator.js';
import type {PrimaryKey, TableSchema} from './schema.js';
import {first, take, type Stream} from './stream.js';

type Args = {
  parent: Input;
  child: Input;
  storage: Storage;
  parentKey: string;
  childKey: string;
  relationshipName: string;
  hidden: boolean;
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
  readonly #parentKey: string;
  readonly #childKey: string;
  readonly #relationshipName: string;
  readonly #schema: TableSchema;

  #output: Output | null = null;

  constructor({
    parent,
    child,
    storage,
    parentKey,
    childKey,
    relationshipName,
    hidden,
  }: Args) {
    assert(parent !== child, 'Parent and child must be different operators');

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
        [relationshipName]: childSchema,
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

  getSchema(): TableSchema {
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
    assert(this.#output, 'Output not set');

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
        // - Update the parent node.
        // - If the value of the join key changed we need to remove the old relation rows and add the new ones.

        this.#output.push({
          type: 'edit',
          row: change.row,
          oldRow: change.oldRow,
        });

        const oldKeyValue = normalizeUndefined(change.oldRow[this.#parentKey]);
        const newKeyValue = normalizeUndefined(change.row[this.#parentKey]);

        if (newKeyValue !== oldKeyValue) {
          const childrenToRemoveStream = this.#child.cleanup({
            constraint: {
              key: this.#childKey,
              value: oldKeyValue,
            },
          });
          for (const childNode of childrenToRemoveStream) {
            this.#output.push({
              type: 'child',
              // This is the new row since we already changed it in the edit above.
              row: change.row,
              child: {
                relationshipName: this.#relationshipName,
                change: {
                  type: 'remove',
                  node: childNode,
                },
              },
            });
          }

          const childrenToAddStream = this.#child.fetch({
            constraint: {
              key: this.#childKey,
              value: newKeyValue,
            },
          });
          for (const childNode of childrenToAddStream) {
            this.#output.push({
              type: 'child',
              row: change.row,
              child: {
                relationshipName: this.#relationshipName,
                change: {
                  type: 'add',
                  node: childNode,
                },
              },
            });
          }
        }

        const {primaryKey} = this.#parent.getSchema();
        const oldStorageKey = makeStorageKey(
          oldKeyValue,
          primaryKey,
          change.oldRow,
        );
        const newStorageKey = makeStorageKey(
          newKeyValue,
          primaryKey,
          change.row,
        );

        // This can be true for both cases. Even if the join key value didn't
        // change the primary key values might have.
        if (oldStorageKey !== newStorageKey) {
          this.#storage.del(oldStorageKey);
          this.#storage.set(newStorageKey, true);
        }

        break;
      }
      default:
        unreachable(change);
    }
  }

  #pushChild(change: Change): void {
    const pushChildChange = (childRow: Row, change: Change) => {
      assert(this.#output, 'Output not set');

      const parentNodes = this.#parent.fetch({
        constraint: {
          key: this.#parentKey,
          value: childRow[this.#childKey],
        },
      });

      for (const parentNode of parentNodes) {
        const childChange: ChildChange = {
          type: 'child',
          row: parentNode.row,
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
        pushChildChange(change.row, change);
        break;
      case 'edit': {
        const childRow = change.row;
        const oldChildRow = change.oldRow;
        if (
          normalizeUndefined(oldChildRow[this.#childKey]) ===
          normalizeUndefined(childRow[this.#childKey])
        ) {
          // The child row was edited in a way that does not change the relationship.
          // We can therefore just push the change down (wrapped in a child change).
          pushChildChange(childRow, change);
        } else {
          // The child row was edited in a way that changes the relationship. We
          // therefore treat this as a remove from the old row followed by an
          // add to the new row.

          const {relationships} = must(
            first(
              this.#child.fetch({
                constraint: {
                  key: this.#childKey,
                  value: oldChildRow[this.#childKey],
                },
              }),
            ),
          );

          pushChildChange(oldChildRow, {
            type: 'remove',
            node: {
              row: oldChildRow,
              relationships,
            },
          });
          pushChildChange(childRow, {
            type: 'add',
            node: {
              row: childRow,
              relationships,
            },
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
    parentNodeRelations: Record<string, Stream<Node>>,
    mode: ProcessParentMode,
  ): Node {
    const parentKeyValue = normalizeUndefined(parentNodeRow[this.#parentKey]);

    // This storage key tracks the primary keys seen for each unique
    // value joined on. This is used to know when to cleanup a child's state.
    const storageKey = makeStorageKey(
      parentKeyValue,
      this.#parent.getSchema().primaryKey,
      parentNodeRow,
    );

    let method: ProcessParentMode = mode;
    if (mode === 'cleanup') {
      const [, second] = take(
        this.#storage.scan({
          prefix: createPrimaryKeySetStorageKeyPrefix(parentKeyValue),
        }),
        2,
      );
      method = second ? 'fetch' : 'cleanup';
    }

    const childStream = this.#child[method]({
      constraint: {
        key: this.#childKey,
        value: parentKeyValue,
      },
    });

    if (mode === 'fetch') {
      this.#storage.set(storageKey, true);
    } else {
      mode satisfies 'cleanup';
      this.#storage.del(storageKey);
    }

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
export function createPrimaryKeySetStorageKey(
  values: readonly NormalizedValue[],
): string {
  const json = JSON.stringify(['pKeySet', ...values]);
  return json.substring(1, json.length - 1) + ',';
}

export function createPrimaryKeySetStorageKeyPrefix(
  value: NormalizedValue,
): string {
  return createPrimaryKeySetStorageKey([value]);
}

function makeStorageKey(
  keyValue: NormalizedValue,
  primaryKey: PrimaryKey,
  row: Row,
): string {
  const parentPrimaryKey: NormalizedValue[] = [keyValue];
  for (const key of primaryKey) {
    parentPrimaryKey.push(normalizeUndefined(row[key]));
  }
  return createPrimaryKeySetStorageKey(parentPrimaryKey);
}
