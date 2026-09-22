import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {CompoundKey, System} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeChildChange,
  makeEditChange,
  makeRemoveChange,
  type Change,
} from './change.ts';
import type {Node} from './data.ts';
import {
  buildJoinConstraint,
  canonicalKey,
  generateWithOverlay,
  generateWithOverlayUnordered,
  isJoinMatch,
  rowEqualsForCompoundKey,
} from './join-utils.ts';
import {mergeSortedStreams} from './memory-source.ts';
import {MemoryStorage} from './memory-storage.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type Output,
  type Storage,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream} from './stream.ts';
import type {TakeBoundProvider} from './take-gate.ts';

export type PartitionEntry = {
  constraint: Record<string, Value>;
  pks: string[];
};

interface JoinStorage {
  get(key: string): PartitionEntry | undefined;
  set(key: string, value: PartitionEntry): void;
  del(key: string): void;
  scan(options?: {prefix: string}): Stream<[string, PartitionEntry]>;
}

function makeJunctionPrefix(junctionKey: string): string {
  return `j\x00${junctionKey}\x00`;
}

function makePartitionStorageKey(
  junctionKey: string,
  partitionKey: string,
): string {
  return `j\x00${junctionKey}\x00${partitionKey}`;
}

type Args = {
  parent: Input;
  child: Input;
  // The nth key in parentKey corresponds to the nth key in childKey.
  parentKey: CompoundKey;
  childKey: CompoundKey;
  relationshipName: string;
  hidden: boolean;
  system: System;
  parentPartitionKey?: CompoundKey | undefined;
  boundProvider?: TakeBoundProvider | undefined;
  trackPartitions?: boolean | undefined;
  storage?: Storage | undefined;
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
  readonly #parentPartitionKey: CompoundKey | undefined;
  readonly #storage: JoinStorage | undefined;
  readonly #boundProvider: TakeBoundProvider | undefined;

  #output: Output = throwOutput;

  #inprogressChildChange: Change | undefined;
  #inprogressChildChangePosition: Row | undefined;

  constructor({
    parent,
    child,
    parentKey,
    childKey,
    relationshipName,
    hidden,
    system,
    parentPartitionKey,
    boundProvider,
    trackPartitions,
    storage,
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
    this.#parentPartitionKey = parentPartitionKey;
    this.#storage =
      (trackPartitions ?? true) && parentPartitionKey
        ? ((storage ?? new MemoryStorage()) as unknown as JoinStorage)
        : undefined;
    this.#boundProvider = boundProvider;

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
    for (const parentNode of this.#parent.fetch(req)) {
      if (parentNode === 'yield') {
        yield parentNode;
        continue;
      }
      this.#indexParentRow(parentNode.row);
      yield this.#processParentNode(parentNode.row, parentNode.relationships);
    }
  }

  *#pushParent(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        this.#indexParentRow(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeAddChange(
            this.#processParentNode(
              change[ChangeIndex.NODE].row,
              change[ChangeIndex.NODE].relationships,
            ),
          ),
          this,
        );
        break;
      case ChangeType.REMOVE:
        this.#unindexParentRow(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeRemoveChange(
            this.#processParentNode(
              change[ChangeIndex.NODE].row,
              change[ChangeIndex.NODE].relationships,
            ),
          ),
          this,
        );
        break;
      case ChangeType.CHILD:
        yield* this.#output.push(
          makeChildChange(
            this.#processParentNode(
              change[ChangeIndex.NODE].row,
              change[ChangeIndex.NODE].relationships,
            ),
            change[ChangeIndex.CHILD_DATA],
          ),
          this,
        );
        break;
      case ChangeType.EDIT: {
        // Assert the edit could not change the relationship.
        assert(
          rowEqualsForCompoundKey(
            change[ChangeIndex.OLD_NODE].row,
            change[ChangeIndex.NODE].row,
            this.#parentKey,
          ),
          `Parent edit must not change relationship.`,
        );
        this.#unindexParentRow(change[ChangeIndex.OLD_NODE].row);
        this.#indexParentRow(change[ChangeIndex.NODE].row);
        yield* this.#output.push(
          makeEditChange(
            this.#processParentNode(
              change[ChangeIndex.NODE].row,
              change[ChangeIndex.NODE].relationships,
            ),
            this.#processParentNode(
              change[ChangeIndex.OLD_NODE].row,
              change[ChangeIndex.OLD_NODE].relationships,
            ),
          ),
          this,
        );
        break;
      }
      default:
        unreachable(change);
    }
  }

  *#pushChild(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
      case ChangeType.REMOVE:
        yield* this.#pushChildChange(change[ChangeIndex.NODE].row, change);
        break;
      case ChangeType.CHILD:
        yield* this.#pushChildChange(change[ChangeIndex.NODE].row, change);
        break;
      case ChangeType.EDIT: {
        const childRow = change[ChangeIndex.NODE].row;
        const oldChildRow = change[ChangeIndex.OLD_NODE].row;
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
    this.#inprogressChildChange = change;
    this.#inprogressChildChangePosition = undefined;
    try {
      const constraint = buildJoinConstraint(
        childRow,
        this.#childKey,
        this.#parentKey,
      );
      if (constraint) {
        let parentNodeStream: Stream<Node | 'yield'>;
        if (this.#storage && this.#parentPartitionKey) {
          const junctionKey = canonicalKey(childRow, this.#childKey);
          const prefix = makeJunctionPrefix(junctionKey);
          const partitionEntries: PartitionEntry[] = [];
          for (const [, entry] of this.#storage.scan({prefix})) {
            partitionEntries.push(entry);
          }
          if (partitionEntries.length === 0) {
            return;
          }
          if (partitionEntries.length === 1) {
            const [entry] = partitionEntries;
            parentNodeStream = this.#parent.fetch({
              constraint: {...constraint, ...entry.constraint},
            });
          } else {
            const streams = partitionEntries.map(entry =>
              this.#parent.fetch({
                constraint: {...constraint, ...entry.constraint},
              }),
            );
            const compare = (a: Node, b: Node) =>
              this.#schema.compareRows(a.row, b.row);
            parentNodeStream = mergeSortedStreams(streams, compare);
          }
        } else {
          parentNodeStream = this.#parent.fetch({constraint});
        }

        for (const parentNode of parentNodeStream) {
          if (parentNode === 'yield') {
            yield parentNode;
            continue;
          }
          this.#inprogressChildChangePosition = parentNode.row;
          const childChange = makeChildChange(
            this.#processParentNode(parentNode.row, parentNode.relationships),
            {
              relationshipName: this.#relationshipName,
              change,
            },
          );
          yield* this.#output.push(childChange, this);
        }
      }
    } finally {
      this.#inprogressChildChange = undefined;
      this.#inprogressChildChangePosition = undefined;
    }
  }

  #indexParentRow(row: Row): void {
    if (
      !this.#storage ||
      !this.#parentPartitionKey ||
      this.#parentKey.some(k => row[k] === null)
    ) {
      return;
    }
    const junctionKey = canonicalKey(row, this.#parentKey);
    const partitionKey = canonicalKey(row, this.#parentPartitionKey);
    const parentPk = canonicalKey(row, this.#parent.getSchema().primaryKey);
    const storageKey = makePartitionStorageKey(junctionKey, partitionKey);
    const entry = this.#storage.get(storageKey);
    if (!entry) {
      this.#storage.set(storageKey, {
        constraint: Object.fromEntries(
          this.#parentPartitionKey.map(k => [k, row[k]]),
        ),
        pks: [parentPk],
      });
    } else if (!entry.pks.includes(parentPk)) {
      entry.pks.push(parentPk);
      this.#storage.set(storageKey, entry);
    }
  }

  #unindexParentRow(row: Row): void {
    if (
      !this.#storage ||
      !this.#parentPartitionKey ||
      this.#parentKey.some(k => row[k] === null)
    ) {
      return;
    }
    const junctionKey = canonicalKey(row, this.#parentKey);
    const partitionKey = canonicalKey(row, this.#parentPartitionKey);
    const parentPk = canonicalKey(row, this.#parent.getSchema().primaryKey);
    const storageKey = makePartitionStorageKey(junctionKey, partitionKey);
    const entry = this.#storage.get(storageKey);
    if (entry) {
      const idx = entry.pks.indexOf(parentPk);
      if (idx !== -1) {
        entry.pks.splice(idx, 1);
        if (entry.pks.length === 0) {
          this.#storage.del(storageKey);
        } else {
          this.#storage.set(storageKey, entry);
        }
      }
    }
  }

  #processParentNode(
    parentNodeRow: Row,
    parentNodeRelations: Record<string, () => Stream<Node | 'yield'>>,
  ): Node {
    const childStream = () => {
      const constraint = buildJoinConstraint(
        parentNodeRow,
        this.#parentKey,
        this.#childKey,
      );
      const stream = constraint ? this.#child.fetch({constraint}) : [];

      let inPushQueue: boolean;
      if (this.#boundProvider) {
        const partitionConstraint = this.#parentPartitionKey
          ? Object.fromEntries(
              this.#parentPartitionKey.map(k => [k, parentNodeRow[k]]),
            )
          : undefined;
        const bound = this.#boundProvider.getBound(partitionConstraint);
        inPushQueue =
          bound !== undefined &&
          this.#inprogressChildChangePosition !== undefined &&
          this.#schema.compareRows(
            parentNodeRow,
            this.#inprogressChildChangePosition,
          ) > 0 &&
          this.#schema.compareRows(parentNodeRow, bound) <= 0;
      } else {
        inPushQueue =
          this.#inprogressChildChangePosition !== undefined &&
          this.#schema.compareRows(
            parentNodeRow,
            this.#inprogressChildChangePosition,
          ) > 0;
      }

      if (
        this.#inprogressChildChange &&
        isJoinMatch(
          parentNodeRow,
          this.#parentKey,
          this.#inprogressChildChange[ChangeIndex.NODE].row,
          this.#childKey,
        ) &&
        inPushQueue
      ) {
        const childSchema = this.#child.getSchema();
        if (childSchema.sort === undefined) {
          return generateWithOverlayUnordered(
            stream,
            this.#inprogressChildChange,
            childSchema,
          );
        }
        return generateWithOverlay(
          stream,
          this.#inprogressChildChange,
          childSchema,
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
