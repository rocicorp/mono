import {assert, unreachable} from '../../../shared/src/asserts.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {Change, ChildChange} from './change.ts';
import type {Node} from './data.ts';
import {
  generateWithOverlay,
  isJoinMatch,
  rowEqualsForCompoundKey,
  type JoinChangeOverlay,
} from './join-utils.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';

type Args = {
  parent: Input;
  child: Input;
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
  readonly #parentKey: CompoundKey;
  readonly #childKey: CompoundKey;
  readonly #relationshipName: string;
  readonly #schema: SourceSchema;

  #output: Output = throwOutput;

  #inprogressChildChange: JoinChangeOverlay | undefined;

  constructor({
    parent,
    child,
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
    this.#parentKey = parentKey;
    this.#childKey = childKey;
    this.#relationshipName = relationshipName;

    const parentSchema = parent.getSchema();
    const childSchema = child.getSchema();
    this.#schema = {
      ...parentSchema,
      relationships: {
        ...parentSchema.relationships,
        [relationshipName]: {
          ...childSchema,
          isHidden: hidden,
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

  cleanup(_req: FetchRequest): Stream<Node> {
    return emptyArray;
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
      yield this.#processParentNode(parentNode.row, parentNode.relationships);
    }
  }

  #pushParent(change: Change): void {
    switch (change.type) {
      case 'add':
        this.#output.push(
          {
            type: 'add',
            node: this.#processParentNode(
              change.node.row,
              change.node.relationships,
            ),
          },
          this,
        );
        break;
      case 'remove':
        this.#output.push(
          {
            type: 'remove',
            node: this.#processParentNode(
              change.node.row,
              change.node.relationships,
            ),
          },
          this,
        );
        break;
      case 'child':
        this.#output.push(
          {
            type: 'child',
            node: this.#processParentNode(
              change.node.row,
              change.node.relationships,
            ),
            child: change.child,
          },
          this,
        );
        break;
      case 'edit': {
        // Assert the edit could not change the relationship.
        assert(
          rowEqualsForCompoundKey(
            change.oldNode.row,
            change.node.row,
            this.#parentKey,
          ),
          `Parent edit must not change relationship.`,
        );
        this.#output.push(
          {
            type: 'edit',
            oldNode: this.#processParentNode(
              change.oldNode.row,
              change.oldNode.relationships,
            ),
            node: this.#processParentNode(
              change.node.row,
              change.node.relationships,
            ),
          },
          this,
        );
        break;
      }
      default:
        unreachable(change);
    }
  }

  #pushChild(change: Change): void {
    const pushChildChange = (childRow: Row, change: Change) => {
      this.#inprogressChildChange = {
        change,
        position: undefined,
      };
      try {
        const parentNodes = this.#parent.fetch({
          constraint: Object.fromEntries(
            this.#parentKey.map((key, i) => [key, childRow[this.#childKey[i]]]),
          ),
        });

        for (const parentNode of parentNodes) {
          this.#inprogressChildChange.position = parentNode.row;
          const childChange: ChildChange = {
            type: 'child',
            node: this.#processParentNode(
              parentNode.row,
              parentNode.relationships,
            ),
            child: {
              relationshipName: this.#relationshipName,
              change,
            },
          };
          this.#output.push(childChange, this);
        }
      } finally {
        this.#inprogressChildChange = undefined;
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
        // Assert the edit could not change the relationship.
        assert(
          rowEqualsForCompoundKey(oldChildRow, childRow, this.#childKey),
          'Child edit must not change relationship.',
        );
        pushChildChange(childRow, change);
        break;
      }

      default:
        unreachable(change);
    }
  }

  #processParentNode(
    parentNodeRow: Row,
    parentNodeRelations: Record<string, () => Stream<Node>>,
  ): Node {
    const childStream = () => {
      const stream = this.#child.fetch({
        constraint: Object.fromEntries(
          this.#childKey.map((key, i) => [
            key,
            parentNodeRow[this.#parentKey[i]],
          ]),
        ),
      });

      if (
        this.#inprogressChildChange &&
        isJoinMatch(
          parentNodeRow,
          this.#parentKey,
          this.#inprogressChildChange.change.node.row,
          this.#childKey,
        ) &&
        this.#inprogressChildChange.position &&
        this.#schema.compareRows(
          parentNodeRow,
          this.#inprogressChildChange.position,
        ) > 0
      ) {
        return generateWithOverlay(
          stream,
          this.#inprogressChildChange.change,
          this.#child.getSchema(),
        );
      }
      return stream;
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
