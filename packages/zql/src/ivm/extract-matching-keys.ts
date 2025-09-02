import {assert} from '../../../shared/src/asserts.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {Change, ChildChange} from './change.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

export interface ExtractMatchingKeysArgs {
  input: Input;
  targetTable: string;
  targetPath: string[];
  targetSchema: SourceSchema;
}

/**
 * ExtractMatchingKeys extracts full row data of a target table from a
 * potentially deeply nested tree structure created by reordered joins.
 *
 * This is used after join reordering for EXISTS queries to extract the
 * root table rows that have matches, allowing us to optimize join order
 * while still returning the correct structure.
 *
 * Example:
 * Input tree (after baz → bar → foo reordering):
 * {
 *   row: { baz_id: 1, baz_data: 'x' },
 *   relationships: {
 *     bar: () => [{
 *       row: { bar_id: 10, bar_data: 'y' },
 *       relationships: {
 *         foo: () => [{
 *           row: { foo_id: 100, foo_name: 'test', foo_value: 42 },
 *           relationships: {}
 *         }]
 *       }
 *     }]
 *   }
 * }
 *
 * Output (with targetTable='foo', targetPath=['bar', 'foo']):
 * { row: { foo_id: 100, foo_name: 'test', foo_value: 42 }, relationships: {} }
 */
export class ExtractMatchingKeys implements Operator {
  readonly #input: Input;
  readonly #targetPath: string[];
  readonly #targetSchema: SourceSchema;
  readonly #targetPrimaryKey: PrimaryKey;
  #output: Output = throwOutput;

  constructor(args: ExtractMatchingKeysArgs) {
    this.#input = args.input;
    this.#targetPath = args.targetPath;
    this.#targetSchema = args.targetSchema;
    this.#targetPrimaryKey = args.targetSchema.primaryKey;

    args.input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    // Return the target table's schema since that's what we're extracting
    return this.#targetSchema;
  }

  *fetch(req: FetchRequest): Stream<Node> {
    const seen = new Set<string>();

    for (const node of this.#input.fetch(req)) {
      yield* this.#extractFromNode(node, [], seen);
    }
  }

  *cleanup(req: FetchRequest): Stream<Node> {
    const seen = new Set<string>();

    for (const node of this.#input.cleanup(req)) {
      yield* this.#extractFromNode(node, [], seen);
    }
  }

  *#extractFromNode(
    node: Node,
    currentPath: string[],
    seen: Set<string>,
  ): Stream<Node> {
    // Check if we've reached the target depth
    if (currentPath.length === this.#targetPath.length) {
      // This node should have our target table data
      const pkKey = this.#getPrimaryKeyString(node.row);

      // Deduplicate (same PK might appear multiple times due to joins)
      if (!seen.has(pkKey)) {
        seen.add(pkKey);
        yield {
          row: node.row, // Full row data
          relationships: {}, // Empty relationships since we're extracting just the row
        };
      }
      return;
    }

    // We need to traverse deeper
    const nextRelName = this.#targetPath[currentPath.length];
    const nextRelStream = node.relationships[nextRelName];

    if (nextRelStream) {
      for (const childNode of nextRelStream()) {
        yield* this.#extractFromNode(
          childNode,
          [...currentPath, nextRelName],
          seen,
        );
      }
    }
    // If the relationship doesn't exist, this branch doesn't have matches
  }

  #getPrimaryKeyString(row: Row): string {
    const pkValues = this.#targetPrimaryKey.map(k => row[k]);
    return JSON.stringify(pkValues);
  }

  push(change: Change): void {
    // For push, we need to traverse the change to find affected target rows
    switch (change.type) {
      case 'add':
      case 'remove':
        this.#pushExtractedChanges(change.type, change.node);
        break;

      case 'edit':
        // For edits, we need to extract from both old and new nodes
        this.#pushExtractedEditChanges(change.oldNode, change.node);
        break;

      case 'child':
        // Follow the child change path to see if it affects our target
        this.#pushChildChange(change);
        break;
    }
  }

  #pushExtractedChanges(type: 'add' | 'remove', node: Node): void {
    const seen = new Set<string>();
    const extracted = [...this.#extractFromNode(node, [], seen)];

    for (const extractedNode of extracted) {
      this.#output.push({
        type,
        node: extractedNode,
      });
    }
  }

  #pushExtractedEditChanges(oldNode: Node, newNode: Node): void {
    const oldSeen = new Set<string>();
    const newSeen = new Set<string>();

    const oldExtracted = [...this.#extractFromNode(oldNode, [], oldSeen)];
    const newExtracted = [...this.#extractFromNode(newNode, [], newSeen)];

    // Match up edits by primary key
    const oldByPk = new Map<string, Node>();
    const newByPk = new Map<string, Node>();

    for (const node of oldExtracted) {
      oldByPk.set(this.#getPrimaryKeyString(node.row), node);
    }

    for (const node of newExtracted) {
      newByPk.set(this.#getPrimaryKeyString(node.row), node);
    }

    // Find edits (present in both)
    for (const [pk, newNode] of newByPk) {
      const oldNode = oldByPk.get(pk);
      if (oldNode) {
        this.#output.push({
          type: 'edit',
          oldNode,
          node: newNode,
        });
        oldByPk.delete(pk);
      } else {
        // New row appeared
        this.#output.push({
          type: 'add',
          node: newNode,
        });
      }
    }

    // Remaining old rows were removed
    for (const oldNode of oldByPk.values()) {
      this.#output.push({
        type: 'remove',
        node: oldNode,
      });
    }
  }

  #pushChildChange(change: ChildChange): void {
    // Check if this child change is on our path to the target
    const childRelName = change.child.relationshipName;
    const pathIndex = this.#targetPath.indexOf(childRelName);

    if (pathIndex === -1) {
      // This child change is not on our path, ignore it
      return;
    }

    // The child change is on our path
    // We need to traverse from the current node following the path
    // up to the child change, then handle the child change

    if (pathIndex === 0) {
      // The child change is at the first level of our path
      // We can directly handle it
      this.#handleChildChangeAtLevel(change.node, change.child.change, 0);
    } else {
      // The child change is deeper in our path
      // This shouldn't happen if flat joins are at the top of the pipeline
      // as documented, but handle it gracefully
      assert(
        false,
        'ExtractMatchingKeys expects to be used after flat joins at the top of the pipeline',
      );
    }
  }

  #handleChildChangeAtLevel(
    _parentNode: Node,
    childChange: Change,
    currentLevel: number,
  ): void {
    if (currentLevel === this.#targetPath.length - 1) {
      // The child change is at our target level
      // Extract the changes directly
      switch (childChange.type) {
        case 'add':
        case 'remove':
          this.#output.push({
            type: childChange.type,
            node: {
              row: childChange.node.row,
              relationships: {},
            },
          });
          break;

        case 'edit':
          this.#output.push({
            type: 'edit',
            oldNode: {
              row: childChange.oldNode.row,
              relationships: {},
            },
            node: {
              row: childChange.node.row,
              relationships: {},
            },
          });
          break;

        case 'child':
          // Nested child change deeper than our target, ignore
          break;
      }
    } else {
      // Need to continue traversing
      // This is complex and shouldn't happen with our pipeline structure
      assert(
        false,
        'ExtractMatchingKeys does not support nested child changes in the middle of the target path',
      );
    }
  }

  destroy(): void {
    this.#input.destroy();
  }
}
