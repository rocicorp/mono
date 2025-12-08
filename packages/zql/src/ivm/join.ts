import {areEqual} from '../../../shared/src/arrays.ts';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change, ChildChange} from './change.ts';
import {normalizeUndefined, type Node, type NormalizedValue} from './data.ts';

/**
 * After this many cache lookups, check if caching is worthwhile.
 */
const CACHE_SAMPLE_SIZE = 100;

/**
 * Minimum hit rate to continue caching after sample size is reached.
 */
const CACHE_MIN_HIT_RATE = 0.1;

/**
 * Maximum number of nodes to cache across all entries.
 * Prevents unbounded memory growth.
 */
const MAX_CACHED_NODES = 10_000;
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
  // The nth key in parentKey corresponds to the nth key in childKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;
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

  /**
   * If true, the parent key equals the parent's primary key,
   * meaning every parent row has a unique key value - no cache benefit.
   */
  readonly #parentKeyIsPrimaryKey: boolean;

  #output: Output = throwOutput;

  #inprogressChildChange: JoinChangeOverlay | undefined;

  // Fetch-time child cache state (only active during fetch)
  #fetchChildCache: Map<string, Node[]> | undefined;
  #cacheLookups = 0;
  #cacheHits = 0;
  #cachedNodeCount = 0;

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

    // If parent key equals parent's primary key, every row is unique - no cache benefit
    this.#parentKeyIsPrimaryKey = areEqual(
      [...this.#parentKey].sort(),
      [...parentSchema.primaryKey].sort(),
    );

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

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    // Initialize cache if it could be useful
    if (!this.#parentKeyIsPrimaryKey) {
      this.#fetchChildCache = new Map();
      this.#cacheLookups = 0;
      this.#cacheHits = 0;
      this.#cachedNodeCount = 0;
    }
    try {
      for (const parentNode of this.#parent.fetch(req)) {
        if (parentNode === 'yield') {
          yield parentNode;
          continue;
        }
        yield this.#processParentNode(parentNode.row, parentNode.relationships);
      }
    } finally {
      this.#fetchChildCache = undefined;
    }
  }

  *#pushParent(change: Change): Stream<'yield'> {
    switch (change.type) {
      case 'add':
        yield* this.#output.push(
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
        yield* this.#output.push(
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
        yield* this.#output.push(
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
        yield* this.#output.push(
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

  *#pushChild(change: Change): Stream<'yield'> {
    switch (change.type) {
      case 'add':
      case 'remove':
        yield* this.#pushChildChange(change.node.row, change);
        break;
      case 'child':
        yield* this.#pushChildChange(change.node.row, change);
        break;
      case 'edit': {
        const childRow = change.node.row;
        const oldChildRow = change.oldNode.row;
        // Assert the edit could not change the relationship.
        assert(
          rowEqualsForCompoundKey(oldChildRow, childRow, this.#childKey),
          'Child edit must not change relationship.',
        );
        yield* this.#pushChildChange(childRow, change);
        break;
      }

      default:
        unreachable(change);
    }
  }

  *#pushChildChange(childRow: Row, change: Change): Stream<'yield'> {
    this.#inprogressChildChange = {
      change,
      position: undefined,
    };
    try {
      let anyNull = false;
      const constraint = Object.fromEntries(
        this.#parentKey.map((key, i) => {
          const value = childRow[this.#childKey[i]];
          if (value === null) {
            anyNull = true;
          }
          return [key, value];
        }),
      );
      const parentNodes = anyNull
        ? []
        : this.#parent.fetch({
            constraint,
          });

      for (const parentNode of parentNodes) {
        if (parentNode === 'yield') {
          yield parentNode;
          continue;
        }
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
        yield* this.#output.push(childChange, this);
      }
    } finally {
      this.#inprogressChildChange = undefined;
    }
  }

  #processParentNode(
    parentNodeRow: Row,
    parentNodeRelations: Record<string, () => Stream<Node | 'yield'>>,
  ): Node {
    const childStream = (): Stream<Node | 'yield'> => {
      let anyNull = false;
      const constraint = Object.fromEntries(
        this.#childKey.map((key, i) => {
          const value = parentNodeRow[this.#parentKey[i]];
          if (value === null) {
            anyNull = true;
          }
          return [key, value];
        }),
      );

      if (anyNull) {
        return [];
      }

      const stream = this.#child.fetch({constraint});

      // Overlay logic for push (only active during push, not fetch)
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

      // Cache logic (only active during fetch when cache exists)
      if (this.#fetchChildCache) {
        const cacheKey = this.#makeCacheKey(parentNodeRow);
        const cached = this.#fetchChildCache.get(cacheKey);

        this.#cacheLookups++;
        if (cached !== undefined) {
          this.#cacheHits++;
          return cached;
        }

        // After sample size, check once if caching is worthwhile
        if (this.#cacheLookups === CACHE_SAMPLE_SIZE) {
          const hitRate = this.#cacheHits / this.#cacheLookups;
          if (hitRate < CACHE_MIN_HIT_RATE) {
            // Not worth it - disable cache for rest of this fetch
            this.#fetchChildCache = undefined;
            return stream;
          }
        }

        return this.#generateAndCache(cacheKey, stream);
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

  #makeCacheKey(parentRow: Row): string {
    const values: NormalizedValue[] = [];
    for (const key of this.#parentKey) {
      values.push(normalizeUndefined(parentRow[key]));
    }
    return JSON.stringify(values);
  }

  *#generateAndCache(
    cacheKey: string,
    stream: Stream<Node | 'yield'>,
  ): Stream<Node | 'yield'> {
    const nodes: Node[] = [];
    for (const node of stream) {
      if (node === 'yield') {
        yield 'yield';
      } else {
        nodes.push(node);
        yield node;
      }
    }

    // Only cache if within memory budget
    if (
      this.#fetchChildCache &&
      this.#cachedNodeCount + nodes.length <= MAX_CACHED_NODES
    ) {
      this.#fetchChildCache.set(cacheKey, nodes);
      this.#cachedNodeCount += nodes.length;
    }
  }
}
