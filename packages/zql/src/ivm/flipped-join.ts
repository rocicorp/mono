import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import {compareValues, valuesEqual, type Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {first, type Stream} from './stream.ts';

type Args = {
  parent: Input;
  child: Input;
  // The order of the keys does not have to match but the length must match.
  // The nth key in parentKey corresponds to the nth key in childKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;

  relationshipName: string;
  hidden: boolean;
  system: System;
};

type ParentChangeOverlay = {
  change: Change;
  position: Row | undefined;
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
export class FlippedJoin implements Input {
  readonly #parent: Input;
  readonly #child: Input;
  readonly #parentKey: CompoundKey;
  readonly #childKey: CompoundKey;
  readonly #relationshipName: string;
  readonly #schema: SourceSchema;

  #output: Output = throwOutput;

  #inprogressParentChange: ParentChangeOverlay | undefined;

  constructor({
    parent,
    child,
    parentKey,
    childKey,
    relationshipName,
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
      ...childSchema,
      relationships: {
        ...childSchema.relationships,
        [relationshipName]: {
          ...parentSchema,
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
    // TODO: optimized version for cardinality one child relationships
    const parentNodes = [...this.#parent.fetch({})];
    const childIterators = [];
    let threw = false;
    try {
      for (const parentNode of parentNodes) {
        // TODO can there be conflicts between req.constraint
        // and the constraint for the child fetch?
        const stream = this.#child.fetch({
          ...req,
          constraint: {
            ...req.constraint,
            ...Object.fromEntries(
              this.#childKey.map((key, i) => [
                key,
                parentNode.row[this.#parentKey[i]],
              ]),
            ),
          },
        });
        const iterator = stream[Symbol.iterator]();
        childIterators.push(iterator);
      }
      const nextChildNodes: (Node | null)[] = [];
      for (let i = 0; i < childIterators.length; i++) {
        const iter = childIterators[i];
        const result = iter.next();
        nextChildNodes[i] = result.done ? null : result.value;
      }

      while (true) {
        let minChildNode = null;
        let minChildNodeParentIndexes: number[] = [];
        for (let i = 0; i < nextChildNodes.length; i++) {
          const childNode = nextChildNodes[i];
          if (childNode === null) {
            continue;
          }
          if (minChildNode === null) {
            minChildNode = childNode;
            minChildNodeParentIndexes.push(i);
          } else {
            const compareResult =
              this.#schema.compareRows(childNode.row, minChildNode.row) *
              (req.reverse ? -1 : 1);
            if (compareResult === 0) {
              minChildNodeParentIndexes.push(i);
            } else if (compareResult < 0) {
              minChildNode = childNode;
              minChildNodeParentIndexes = [i];
            }
          }
        }
        if (minChildNode === null) {
          return;
        }
        const relationshipNodes: Node[] = [];
        for (const minChildNodeParentIndex of minChildNodeParentIndexes) {
          relationshipNodes.push(parentNodes[minChildNodeParentIndex]);
          const iter = childIterators[minChildNodeParentIndex];
          const result = iter.next();
          nextChildNodes[minChildNodeParentIndex] = result.done
            ? null
            : result.value;
        }
        const overlaidRelationshipNodes =
          this.#inprogressParentChange &&
          this.#inprogressParentChange.position &&
          this.#isJoinMatch(
            minChildNode.row,
            this.#inprogressParentChange.change.node.row,
          ) &&
          this.#schema.compareRows(
            minChildNode.row,
            this.#inprogressParentChange.position,
          ) > 0
            ? [
                ...this.#overlayChange(
                  relationshipNodes,
                  this.#inprogressParentChange?.change,
                ),
              ]
            : relationshipNodes;
        // yield node if after the overlay it still has relationship nodes
        if (overlaidRelationshipNodes.length > 0) {
          yield {
            ...minChildNode,
            relationships: {
              ...minChildNode.relationships,
              [this.#relationshipName]: () => overlaidRelationshipNodes,
            },
          };
        }
      }
    } catch (e) {
      threw = true;
      for (const iter of childIterators) {
        // TODO should this be a new error?
        iter.throw?.(e);
      }
      throw e;
    } finally {
      if (!threw) {
        for (const iter of childIterators) {
          iter.return?.();
        }
      }
    }
  }

  *cleanup(_req: FetchRequest): Stream<Node> {}

  #pushParent(change: Change): void {
    const pushParentChange = (exists?: boolean) => {
      this.#inprogressParentChange = {
        change,
        position: undefined,
      };
      try {
        const childNodeStream = this.#child.fetch({
          constraint: Object.fromEntries(
            this.#childKey.map((key, i) => [
              key,
              change.node.row[this.#parentKey[i]],
            ]),
          ),
        });
        for (const childNode of childNodeStream) {
          this.#inprogressParentChange = {
            change,
            position: childNode.row,
          };
          const parentNodeStream = () =>
            this.#parent.fetch({
              constraint: Object.fromEntries(
                this.#parentKey.map((key, i) => [
                  key,
                  childNode.row[this.#childKey[i]],
                ]),
              ),
            });
          if (!exists) {
            for (const parentNode of parentNodeStream()) {
              if (
                this.#parent
                  .getSchema()
                  .compareRows(parentNode.row, change.node.row) !== 0
              ) {
                exists = true;
                break;
              }
            }
          }
          if (exists) {
            this.#output.push({
              type: 'child',
              node: {
                ...childNode,
                relationships: {
                  ...childNode.relationships,
                  [this.#relationshipName]: parentNodeStream,
                },
              },
              child: {
                relationshipName: this.#relationshipName,
                change,
              },
            });
          } else {
            this.#output.push({
              ...change,
              node: {
                ...childNode,
                relationships: {
                  ...childNode.relationships,
                  [this.#relationshipName]: () => [change.node],
                },
              },
            });
          }
        }
      } finally {
        this.#inprogressParentChange = undefined;
      }
    };

    switch (change.type) {
      case 'add':
      case 'remove':
        pushParentChange();
        break;
      case 'edit': {
        assert(
          rowEqualsForCompoundKey(
            change.oldNode.row,
            change.node.row,
            this.#parentKey,
          ),
          `Parent edit must not change relationship.`,
        );
        pushParentChange(true);
        break;
      }
      case 'child':
        pushParentChange(true);
        break;
    }
  }

  #pushChild(change: Change): void {
    const parentNodeStream = (node: Node) => () =>
      this.#parent.fetch({
        constraint: Object.fromEntries(
          this.#parentKey.map((key, i) => [key, node.row[this.#childKey[i]]]),
        ),
      });
    // TODO for edit we need to check oldNode
    if (first(parentNodeStream(change.node)()) === undefined) {
      return;
    }

    const flip = (node: Node) => ({
      ...node,
      relationships: {
        ...node.relationships,
        [this.#relationshipName]: parentNodeStream(node),
      },
    });

    switch (change.type) {
      case 'add':
      case 'remove':
      case 'child': {
        if (first(parentNodeStream(change.node)()) === undefined) {
          return;
        }
        this.#output.push({
          ...change,
          node: flip(change.node),
        });
        break;
      }
      case 'edit': {
        const oldHasParent =
          first(parentNodeStream(change.oldNode)()) !== undefined;
        const hasParent = first(parentNodeStream(change.node)()) !== undefined;
        // Assert the edit could not change the relationship.
        assert(
          rowEqualsForCompoundKey(
            change.oldNode.row,
            change.node.row,
            this.#childKey,
          ),
          `Child edit must not change relationship.`,
        );
        if (oldHasParent && hasParent) {
          this.#output.push({
            type: 'edit',
            oldNode: flip(change.oldNode),
            node: flip(change.node),
          });
          break;
        }
        if (oldHasParent) {
          this.#output.push({
            type: 'remove',
            node: flip(change.node),
          });
        }
        if (hasParent) {
          this.#output.push({
            type: 'add',
            node: flip(change.node),
          });
        }
        break;
      }
      default:
        unreachable(change);
    }
  }

  // TODO share with join
  *#overlayChange(stream: Stream<Node>, overlay: Change): Stream<Node> {
    let applied = false;
    let editOldApplied = false;
    let editNewApplied = false;
    for (const node of stream) {
      let yieldChild = true;
      if (!applied) {
        switch (overlay.type) {
          case 'add': {
            if (
              this.#parent
                .getSchema()
                .compareRows(overlay.node.row, node.row) === 0
            ) {
              applied = true;
              yieldChild = false;
            }
            break;
          }
          case 'remove': {
            if (
              this.#child.getSchema().compareRows(overlay.node.row, node.row) <
              0
            ) {
              applied = true;
              yield overlay.node;
            }
            break;
          }
          case 'edit': {
            if (
              this.#child
                .getSchema()
                .compareRows(overlay.oldNode.row, node.row) < 0
            ) {
              editOldApplied = true;
              if (editNewApplied) {
                applied = true;
              }
              yield overlay.oldNode;
            }
            if (
              this.#child
                .getSchema()
                .compareRows(overlay.node.row, node.row) === 0
            ) {
              editNewApplied = true;
              if (editOldApplied) {
                applied = true;
              }
              yieldChild = false;
            }
            break;
          }
          case 'child': {
            if (
              this.#child
                .getSchema()
                .compareRows(overlay.node.row, node.row) === 0
            ) {
              applied = true;
              yield {
                row: node.row,
                relationships: {
                  ...node.relationships,
                  [overlay.child.relationshipName]: () =>
                    this.#overlayChange(
                      node.relationships[overlay.child.relationshipName](),
                      overlay.child.change,
                    ),
                },
              };
              yieldChild = false;
            }
            break;
          }
        }
      }
      if (yieldChild) {
        yield node;
      }
    }
    if (!applied) {
      if (overlay.type === 'remove') {
        applied = true;
        yield overlay.node;
      } else if (overlay.type === 'edit') {
        assert(editNewApplied);
        editOldApplied = true;
        applied = true;
        yield overlay.oldNode;
      }
    }

    assert(applied);
  }

  #isJoinMatch(parent: Row, child: Row) {
    for (let i = 0; i < this.#parentKey.length; i++) {
      if (!valuesEqual(parent[this.#parentKey[i]], child[this.#childKey[i]])) {
        return false;
      }
    }
    return true;
  }
}

function rowEqualsForCompoundKey(a: Row, b: Row, key: CompoundKey): boolean {
  for (let i = 0; i < key.length; i++) {
    if (compareValues(a[key[i]], b[key[i]]) !== 0) {
      return false;
    }
  }
  return true;
}
