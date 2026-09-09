import {assert} from '../../../shared/src/asserts.ts';
import type {CompoundKey} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import {compareValues, valuesEqual, type Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import {type PullStream} from './stream.ts';

export function generateWithOverlayNoYield(
  stream: PullStream<Node>,
  overlay: Change,
  schema: SourceSchema,
): PullStream<Node> {
  return generateWithOverlay(
    stream as PullStream<Node | 'yield'>,
    overlay,
    schema,
  ) as PullStream<Node>;
}

/**
 * Splices a pending change into a node stream, as a pull stream.
 *
 * One input node can produce two outputs -- the overlay and the node itself --
 * which the generator expressed as two `yield`s in one loop iteration. `#q`
 * holds those so `next()` can hand them back one at a time.
 */
class JoinOverlay implements PullStream<Node | 'yield'> {
  readonly #stream: PullStream<Node | 'yield'>;
  readonly #overlay: Change;
  readonly #schema: SourceSchema;
  readonly #q: (Node | 'yield')[] = [];
  #applied = false;
  #editOldApplied = false;
  #editNewApplied = false;
  #exhausted = false;
  #tailDone = false;

  constructor(
    stream: PullStream<Node | 'yield'>,
    overlay: Change,
    schema: SourceSchema,
  ) {
    this.#stream = stream;
    this.#overlay = overlay;
    this.#schema = schema;
  }

  #step(node: Node): void {
    const overlay = this.#overlay;
    const schema = this.#schema;
    const q = this.#q;
    let yieldNode = true;
    if (!this.#applied) {
      switch (overlay[ChangeIndex.TYPE]) {
        case ChangeType.ADD: {
          if (
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            this.#applied = true;
            yieldNode = false;
          }
          break;
        }
        case ChangeType.REMOVE: {
          if (schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) < 0) {
            this.#applied = true;
            q.push(overlay[ChangeIndex.NODE]);
          }
          break;
        }
        case ChangeType.EDIT: {
          if (
            !this.#editOldApplied &&
            schema.compareRows(overlay[ChangeIndex.OLD_NODE].row, node.row) < 0
          ) {
            this.#editOldApplied = true;
            if (this.#editNewApplied) {
              this.#applied = true;
            }
            q.push(overlay[ChangeIndex.OLD_NODE]);
          }
          if (
            !this.#editNewApplied &&
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            this.#editNewApplied = true;
            if (this.#editOldApplied) {
              this.#applied = true;
            }
            yieldNode = false;
          }
          break;
        }
        case ChangeType.CHILD: {
          if (
            schema.compareRows(overlay[ChangeIndex.NODE].row, node.row) === 0
          ) {
            this.#applied = true;
            q.push({
              row: node.row,
              relationships: {
                ...node.relationships,
                [overlay[ChangeIndex.CHILD_DATA].relationshipName]: () =>
                  generateWithOverlay(
                    node.relationships[
                      overlay[ChangeIndex.CHILD_DATA].relationshipName
                    ](),
                    overlay[ChangeIndex.CHILD_DATA].change,
                    schema.relationships[
                      overlay[ChangeIndex.CHILD_DATA].relationshipName
                    ],
                  ),
              },
            });
            yieldNode = false;
          }
          break;
        }
      }
    }
    if (yieldNode) {
      q.push(node);
    }
  }

  #tail(): void {
    const overlay = this.#overlay;
    if (!this.#applied) {
      if (overlay[ChangeIndex.TYPE] === ChangeType.REMOVE) {
        this.#applied = true;
        this.#q.push(overlay[ChangeIndex.NODE]);
      } else if (overlay[ChangeIndex.TYPE] === ChangeType.EDIT) {
        assert(
          this.#editNewApplied,
          'edit overlay: new node must be applied before old node',
        );
        this.#editOldApplied = true;
        this.#applied = true;
        this.#q.push(overlay[ChangeIndex.OLD_NODE]);
      }
    }
    assert(
      this.#applied,
      'overlayGenerator: overlay was never applied to any fetched node',
    );
  }

  next(): Node | 'yield' | undefined {
    for (;;) {
      if (this.#q.length > 0) {
        return this.#q.shift();
      }
      if (this.#exhausted) {
        if (!this.#tailDone) {
          this.#tailDone = true;
          this.#tail();
          continue;
        }
        return undefined;
      }
      const node = this.#stream.next();
      if (node === undefined) {
        this.#exhausted = true;
        continue;
      }
      if (node === 'yield') {
        return node;
      }
      this.#step(node);
    }
  }

  close(): void {
    this.#exhausted = true;
    this.#tailDone = true;
    this.#q.length = 0;
    this.#stream.close();
  }
}

export function generateWithOverlay(
  stream: PullStream<Node | 'yield'>,
  overlay: Change,
  schema: SourceSchema,
): PullStream<Node | 'yield'> {
  return new JoinOverlay(stream, overlay, schema);
}

export function generateWithOverlayNoYieldUnordered(
  stream: PullStream<Node>,
  overlay: Change,
  schema: SourceSchema,
): PullStream<Node> {
  return generateWithOverlayUnordered(
    stream as PullStream<Node | 'yield'>,
    overlay,
    schema,
  ) as PullStream<Node>;
}

/** {@link JoinOverlay} for unordered streams: eager inject, inline suppress. */
class JoinOverlayUnordered implements PullStream<Node | 'yield'> {
  readonly #stream: PullStream<Node | 'yield'>;
  readonly #overlay: Change;
  readonly #schema: SourceSchema;
  readonly #q: (Node | 'yield')[] = [];
  #injected = false;
  #suppressed = false;
  #done = false;

  constructor(
    stream: PullStream<Node | 'yield'>,
    overlay: Change,
    schema: SourceSchema,
  ) {
    this.#stream = stream;
    this.#overlay = overlay;
    this.#schema = schema;
  }

  #step(node: Node): void {
    const overlay = this.#overlay;
    const schema = this.#schema;
    if (!this.#suppressed) {
      if (
        overlay[ChangeIndex.TYPE] === ChangeType.ADD ||
        overlay[ChangeIndex.TYPE] === ChangeType.EDIT
      ) {
        if (
          rowEqualsForCompoundKey(
            overlay[ChangeIndex.NODE].row,
            node.row,
            schema.primaryKey,
          )
        ) {
          this.#suppressed = true;
          return;
        }
      }
      if (overlay[ChangeIndex.TYPE] === ChangeType.CHILD) {
        if (
          rowEqualsForCompoundKey(
            overlay[ChangeIndex.NODE].row,
            node.row,
            schema.primaryKey,
          )
        ) {
          this.#suppressed = true;
          this.#q.push({
            row: node.row,
            relationships: {
              ...node.relationships,
              [overlay[ChangeIndex.CHILD_DATA].relationshipName]: () =>
                generateWithOverlay(
                  node.relationships[
                    overlay[ChangeIndex.CHILD_DATA].relationshipName
                  ](),
                  overlay[ChangeIndex.CHILD_DATA].change,
                  schema.relationships[
                    overlay[ChangeIndex.CHILD_DATA].relationshipName
                  ],
                ),
            },
          });
          return;
        }
      }
    }
    this.#q.push(node);
  }

  next(): Node | 'yield' | undefined {
    if (!this.#injected) {
      this.#injected = true;
      const overlay = this.#overlay;
      if (overlay[ChangeIndex.TYPE] === ChangeType.REMOVE) {
        this.#q.push(overlay[ChangeIndex.NODE]);
      } else if (overlay[ChangeIndex.TYPE] === ChangeType.EDIT) {
        this.#q.push(overlay[ChangeIndex.OLD_NODE]);
      }
    }
    for (;;) {
      if (this.#q.length > 0) {
        return this.#q.shift();
      }
      if (this.#done) {
        return undefined;
      }
      const node = this.#stream.next();
      if (node === undefined) {
        this.#done = true;
        assert(
          this.#suppressed ||
            this.#overlay[ChangeIndex.TYPE] === ChangeType.REMOVE,
          'overlayGenerator: overlay was never applied to any fetched node',
        );
        return undefined;
      }
      if (node === 'yield') {
        return node;
      }
      this.#step(node);
    }
  }

  close(): void {
    this.#done = true;
    this.#q.length = 0;
    this.#stream.close();
  }
}

export function generateWithOverlayUnordered(
  stream: PullStream<Node | 'yield'>,
  overlay: Change,
  schema: SourceSchema,
): PullStream<Node | 'yield'> {
  return new JoinOverlayUnordered(stream, overlay, schema);
}

export function rowEqualsForCompoundKey(
  a: Row,
  b: Row,
  key: CompoundKey,
): boolean {
  for (let i = 0; i < key.length; i++) {
    if (compareValues(a[key[i]], b[key[i]]) !== 0) {
      return false;
    }
  }
  return true;
}

export function isJoinMatch(
  parent: Row,
  parentKey: CompoundKey,
  child: Row,
  childKey: CompoundKey,
) {
  for (let i = 0; i < parentKey.length; i++) {
    if (!valuesEqual(parent[parentKey[i]], child[childKey[i]])) {
      return false;
    }
  }
  return true;
}

/**
 * Builds a constraint object by mapping values from `sourceRow` using `sourceKey`
 * to keys specified by `targetKey`. Returns `undefined` if any source value is `null`,
 * since null foreign keys cannot match any rows.
 */
export function buildJoinConstraint(
  sourceRow: Row,
  sourceKey: CompoundKey,
  targetKey: CompoundKey,
): Record<string, Value> | undefined {
  const constraint: Record<string, Value> = {};
  for (let i = 0; i < targetKey.length; i++) {
    const value = sourceRow[sourceKey[i]];
    if (value === null) {
      return undefined;
    }
    constraint[targetKey[i]] = value;
  }
  return constraint;
}
