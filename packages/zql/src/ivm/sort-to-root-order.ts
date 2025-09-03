import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Change} from './change.ts';
import {makeComparator, type Comparator, type Node} from './data.ts';
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

export interface SortToRootOrderArgs {
  input: Input;
  storage: Storage;
  targetSort: Ordering;
}

/**
 * SortToRootOrder re-sorts extracted rows to match the root table's
 * sort order. This is necessary after join reordering changes the
 * natural order of results.
 *
 * This operator buffers all results on fetch and sorts them according
 * to the specified ordering. Push operations are passed through unsorted
 * since push is inherently unordered.
 *
 * Example:
 * Input (after extraction, in wrong order due to join reordering):
 * { foo_id: 5, foo_name: 'E' }
 * { foo_id: 2, foo_name: 'B' }
 * { foo_id: 8, foo_name: 'H' }
 *
 * Output (sorted by foo_id asc):
 * { foo_id: 2, foo_name: 'B' }
 * { foo_id: 5, foo_name: 'E' }
 * { foo_id: 8, foo_name: 'H' }
 */
export class SortToRootOrder implements Operator {
  readonly #input: Input;
  readonly #targetSort: Ordering;
  readonly #comparator: Comparator;
  readonly #schema: SourceSchema;
  #output: Output = throwOutput;

  constructor(args: SortToRootOrderArgs) {
    this.#input = args.input;
    // Storage is provided but not currently used
    // Could be used for future optimizations like caching sorted state
    this.#targetSort = args.targetSort;
    this.#comparator = makeComparator(args.targetSort);

    // Update the schema's sort and compareRows to match our target sort
    const inputSchema = this.#input.getSchema();
    this.#schema = {
      ...inputSchema,
      sort: this.#targetSort,
      compareRows: this.#comparator,
    };

    args.input.setOutput(this);
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node> {
    // Buffer all results from input
    const buffered: Node[] = [];
    // Important: pass empty request to input to get ALL rows
    // We'll handle start/reverse ourselves after sorting
    for (const node of this.#input.fetch({})) {
      buffered.push(node);
    }

    // Sort according to target order
    buffered.sort((a, b) => this.#comparator(a.row, b.row));

    // Handle start and reverse parameters
    if (req.start) {
      const startIndex = this.#findStartIndex(buffered, req.start);
      if (req.reverse) {
        // Yield in reverse from start point
        for (let i = startIndex; i >= 0; i--) {
          yield buffered[i];
        }
      } else {
        // Yield forward from start point
        for (let i = startIndex; i < buffered.length; i++) {
          yield buffered[i];
        }
      }
    } else if (req.reverse) {
      // Yield all in reverse order
      for (let i = buffered.length - 1; i >= 0; i--) {
        yield buffered[i];
      }
    } else {
      // Yield all in forward order
      for (const node of buffered) {
        yield node;
      }
    }
  }

  // TODO: correct way to do this?
  *cleanup(_req: FetchRequest): Stream<Node> {
    // For cleanup, we also need to buffer and sort
    const buffered: Node[] = [];
    // Pass empty request to get all rows
    for (const node of this.#input.cleanup({})) {
      buffered.push(node);
    }

    // Sort according to target order
    buffered.sort((a, b) => this.#comparator(a.row, b.row));

    // Yield in sorted order
    // Note: cleanup typically doesn't use start/reverse, but we could support it
    for (const node of buffered) {
      yield node;
    }
  }

  #findStartIndex(nodes: Node[], start: FetchRequest['start']): number {
    if (!start) {
      return 0;
    }

    // Find the position using binary search
    for (let i = 0; i < nodes.length; i++) {
      const cmp = this.#comparator(nodes[i].row, start.row);
      if (cmp === 0) {
        // Found exact match
        return start.basis === 'after' ? i + 1 : i;
      } else if (cmp > 0) {
        // Passed where it would be - for 'at' with no exact match,
        // we start from where it would have been
        return start.basis === 'after' ? i : i;
      }
    }

    // Not found and would be at the end
    return nodes.length;
  }

  push(change: Change): void {
    // Push is inherently unsorted, so we just pass through
    // The downstream operators are responsible for handling the unordered changes
    this.#output.push(change);
  }

  destroy(): void {
    this.#input.destroy();
  }
}
