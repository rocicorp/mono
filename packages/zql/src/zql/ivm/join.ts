import {assert} from 'shared/src/asserts.js';
import type {Change} from './change.js';
import {normalizeUndefined, type Node, type NormalizedValue} from './data.js';
import type {FetchRequest, Input, Output, Storage} from './operator.js';
import type {Schema} from './schema.js';
import {take, type Stream} from './stream.js';

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
  readonly #schema: Schema;

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

  getSchema(): Schema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node> {
    for (const parentNode of this.#parent.fetch(req)) {
      yield this.#processParentNode(parentNode, 'fetch');
    }
  }

  *cleanup(req: FetchRequest): Stream<Node> {
    for (const parentNode of this.#parent.cleanup(req)) {
      yield this.#processParentNode(parentNode, 'cleanup');
    }
  }

  #pushParent(change: Change): void {
    assert(this.#output, 'Output not set');
    if (change.type === 'add') {
      this.#output.push({
        type: 'add',
        node: this.#processParentNode(change.node, 'fetch'),
      });
    } else if (change.type === 'remove') {
      this.#output.push({
        type: 'remove',
        node: this.#processParentNode(change.node, 'cleanup'),
      });
    } else {
      change.type satisfies 'child';
      this.#output.push(change);
    }
  }

  #pushChild(change: Change): void {
    assert(this.#output, 'Output not set');
    const childRow = change.type === 'child' ? change.row : change.node.row;
    const parentNodes = this.#parent.fetch({
      constraint: {
        key: this.#parentKey,
        value: childRow[this.#childKey],
      },
    });

    for (const parentNode of parentNodes) {
      const result: Change = {
        type: 'child',
        row: parentNode.row,
        child: {
          relationshipName: this.#relationshipName,
          change,
        },
      };
      this.#output.push(result);
    }
  }

  #processParentNode(parentNode: Node, mode: ProcessParentMode): Node {
    const parentKeyValue = normalizeUndefined(parentNode.row[this.#parentKey]);
    const parentPrimaryKey: NormalizedValue[] = [];
    for (const key of this.#parent.getSchema().primaryKey) {
      parentPrimaryKey.push(normalizeUndefined(parentNode.row[key]));
    }

    // This storage key tracks the primary keys seen for each unique
    // value joined on. This is used to know when to cleanup a child's state.
    const storageKey: string = createPrimaryKeySetStorageKey([
      parentKeyValue,
      ...parentPrimaryKey,
    ]);

    let method: ProcessParentMode = mode;
    if (mode === 'cleanup') {
      const [, second] = [
        ...take(
          this.#storage.scan({
            prefix: createPrimaryKeySetStorageKeyPrefix(parentKeyValue),
          }),
          2,
        ),
      ];
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
      ...parentNode,
      relationships: {
        ...parentNode.relationships,
        [this.#relationshipName]: childStream,
      },
    };
  }
}

type ProcessParentMode = 'fetch' | 'cleanup';

/** Exported for testing. */
export function createPrimaryKeySetStorageKey(
  values: NormalizedValue[],
): string {
  const json = JSON.stringify(['pKeySet', ...values]);
  return json.substring(1, json.length - 1) + ',';
}

export function createPrimaryKeySetStorageKeyPrefix(
  value: NormalizedValue,
): string {
  return createPrimaryKeySetStorageKey([value]);
}
