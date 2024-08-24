import {assert} from 'shared/src/asserts.js';
import {type Node, normalizeUndefined, type NormalizedValue} from './data.js';
import type {
  FetchRequest,
  Input,
  Operator,
  Output,
  Storage,
} from './operator.js';
import {take, type Stream} from './stream.js';
import type {Change} from './change.js';
import type {Schema} from './schema.js';

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
export class Join implements Operator {
  readonly #parent: Input;
  readonly #child: Input;
  readonly #storage: Storage;
  readonly #parentKey: string;
  readonly #childKey: string;
  readonly #relationshipName: string;

  #output: Output | null = null;

  constructor(
    parent: Input,
    child: Input,
    storage: Storage,
    parentKey: string,
    childKey: string,
    relationshipName: string,
  ) {
    assert(parent !== child, 'Parent and child must be different operators');

    this.#parent = parent;
    this.#child = child;
    this.#storage = storage;
    this.#parentKey = parentKey;
    this.#childKey = childKey;
    this.#relationshipName = relationshipName;

    this.#parent.setOutput(this);
    this.#child.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(_output: Output): Schema {
    return this.#parent.getSchema(this);
  }

  *fetch(req: FetchRequest, _: Output): Stream<Node> {
    for (const parentNode of this.#parent.fetch(req, this)) {
      yield this.#processParentNode(parentNode, 'fetch');
    }
  }

  *cleanup(req: FetchRequest, _: Output): Stream<Node> {
    for (const parentNode of this.#parent.cleanup(req, this)) {
      yield this.#processParentNode(parentNode, 'cleanup');
    }
  }

  push(change: Change, input: Input): void {
    assert(this.#output, 'Output not set');

    if (input === this.#parent) {
      if (change.type === 'add') {
        this.#output.push(
          {
            type: 'add',
            node: this.#processParentNode(change.node, 'fetch'),
          },
          this,
        );
      } else if (change.type === 'remove') {
        this.#output.push(
          {
            type: 'remove',
            node: this.#processParentNode(change.node, 'cleanup'),
          },
          this,
        );
      } else {
        change.type satisfies 'child';
        this.#output.push(change, this);
      }
      return;
    }

    assert(input === this.#child);

    const childRow = change.type === 'child' ? change.row : change.node.row;
    const parentNodes = this.#parent.fetch(
      {
        constraint: {
          key: this.#parentKey,
          value: childRow[this.#childKey],
        },
      },
      this,
    );

    for (const parentNode of parentNodes) {
      const result: Change = {
        type: 'child',
        row: parentNode.row,
        child: {
          relationshipName: this.#relationshipName,
          change,
        },
      };
      this.#output.push(result, this);
    }
  }

  #processParentNode(parentNode: Node, mode: ProcessParentMode): Node {
    const parentKeyValue = normalizeUndefined(parentNode.row[this.#parentKey]);
    const parentPrimaryKey: NormalizedValue[] = [];
    for (const key of this.#parent.getSchema(this).primaryKey) {
      parentPrimaryKey.push(normalizeUndefined(parentNode.row[key]));
    }

    // This storage key tracks of the primary keys we've seen for each unique
    // value of parent key. This is used to know when to cleanup a child,
    // thereby cleaning up its state.
    const storageKey: NormalizedValue[] = [
      'pKeySet',
      parentKeyValue,
      ...parentPrimaryKey,
    ];

    let method: ProcessParentMode = mode;
    if (mode === 'cleanup') {
      const [, second] = [
        ...take(this.#storage.scan({prefix: ['pKeySet', parentKeyValue]}), 2),
      ];
      method = second ? 'fetch' : 'cleanup';
    }

    const childStream = this.#child[method](
      {
        constraint: {
          key: this.#childKey,
          value: parentKeyValue,
        },
      },
      this,
    );

    if (mode === 'fetch') {
      this.#storage.set(storageKey, true);
    } else if (mode === 'cleanup') {
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
