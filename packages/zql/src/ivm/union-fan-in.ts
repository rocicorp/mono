import {assert} from '../../../shared/src/asserts.ts';
import type {Writable} from '../../../shared/src/writable.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import type {Change} from './change.ts';
import type {Constraint} from './constraint.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
} from './operator.ts';
import {
  makeAddEmptyRelationships,
  mergeRelationships,
  pushAccumulatedChanges,
} from './push-accumulated.ts';
import type {SourceSchema} from './schema.ts';
import {type Stream, PullStreamBase, type PullStream} from './stream.ts';
import type {UnionFanOut} from './union-fan-out.ts';

export class UnionFanIn implements Operator {
  readonly #inputs: readonly Input[];
  readonly #schema: SourceSchema;
  #fanOutPushStarted: boolean = false;
  #output: Output = throwOutput;
  #accumulatedPushes: Change[] = [];

  constructor(fanOut: UnionFanOut, inputs: Input[]) {
    this.#inputs = inputs;
    const fanOutSchema = fanOut.getSchema();
    fanOut.setFanIn(this);
    assert(fanOutSchema.sort !== undefined, 'UnionFanIn requires sorted input');

    const schema: Writable<SourceSchema> = {
      tableName: fanOutSchema.tableName,
      columns: fanOutSchema.columns,
      primaryKey: fanOutSchema.primaryKey,
      relationships: {
        ...fanOutSchema.relationships,
      },
      isHidden: fanOutSchema.isHidden,
      system: fanOutSchema.system,
      compareRows: fanOutSchema.compareRows,
      sort: fanOutSchema.sort,
    };

    // now go through inputs and merge relationships
    const relationshipsFromBranches: Set<string> = new Set();
    for (const input of inputs) {
      const inputSchema = input.getSchema();
      assert(
        schema.tableName === inputSchema.tableName,
        `Table name mismatch in union fan-in: ${schema.tableName} !== ${inputSchema.tableName}`,
      );
      assert(
        schema.primaryKey === inputSchema.primaryKey,
        `Primary key mismatch in union fan-in`,
      );
      assert(
        schema.system === inputSchema.system,
        `System mismatch in union fan-in: ${schema.system} !== ${inputSchema.system}`,
      );
      assert(
        schema.compareRows === inputSchema.compareRows,
        `compareRows mismatch in union fan-in`,
      );
      assert(schema.sort === inputSchema.sort, `Sort mismatch in union fan-in`);

      for (const [relName, relSchema] of Object.entries(
        inputSchema.relationships,
      )) {
        if (relName in fanOutSchema.relationships) {
          continue;
        }

        // All branches will have unique relationship names except for relationships
        // that come in from `fanOut`.
        assert(
          !relationshipsFromBranches.has(relName),
          `Relationship ${relName} exists in multiple upstream inputs to union fan-in`,
        );
        schema.relationships[relName] = relSchema;
        relationshipsFromBranches.add(relName);
      }

      input.setOutput(this);
    }

    this.#schema = schema;
    this.#inputs = inputs;
  }

  destroy(): void {
    for (const input of this.#inputs) {
      input.destroy();
    }
  }

  fetch(req: FetchRequest): PullStream<Node | 'yield'> {
    const iterables = this.#inputs.map(input => input.fetch(req));
    const compareRows = this.#schema.compareRows;
    const compare = req.reverse
      ? (l: Node, r: Node) => compareRows(r.row, l.row)
      : (l: Node, r: Node) => compareRows(l.row, r.row);
    return mergeFetches(iterables, compare);
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *push(change: Change, pusher: InputBase): Stream<'yield'> {
    if (!this.#fanOutPushStarted) {
      yield* this.#pushInternalChange(change, pusher);
    } else {
      this.#accumulatedPushes.push(change);
    }
  }

  /**
   * An internal change means that a change was received inside the fan-out/fan-in sub-graph.
   *
   * These changes always come from children of a flip-join as no other push generating operators
   * currently exist between union-fan-in and union-fan-out. All other pushes
   * enter into union-fan-out before reaching union-fan-in.
   *
   * - normal joins for `exists` come before `union-fan-out`
   * - joins for `related` come after `union-fan-out`
   * - take comes after `union-fan-out`
   *
   * The algorithm for deciding whether or not to forward a push that came from inside the ufo/ufi sub-graph:
   * 1. If the change is a `child` change we can forward it. This is because all child branches in the ufo/ufi sub-graph are unique.
   * 2. If the change is `add` we can forward it iff no `fetches` for the row return any results.
   *    If another branch has it, the add was already emitted in the past.
   * 3. If the change is `remove` we can forward it iff no `fetches` for the row return any results.
   *    If no other branches have the change, the remove can be sent as the value is no longer present.
   *    If other branches have it, the last branch the processes the remove will send the remove.
   * 4. Edits will always come through as child changes as flip join will flip them into children.
   *    An edit that would result in a remove or add will have been split into an add/remove pair rather than being an edit.
   */
  *#pushInternalChange(change: Change, pusher: InputBase): Stream<'yield'> {
    if (change[ChangeIndex.TYPE] === ChangeType.CHILD) {
      yield* this.#output.push(change, this);
      return;
    }

    assert(
      change[ChangeIndex.TYPE] === ChangeType.ADD ||
        change[ChangeIndex.TYPE] === ChangeType.REMOVE,
      () =>
        `UnionFanIn: expected add or remove change type, got ${change[ChangeIndex.TYPE]}`,
    );

    let hadMatch = false;
    for (const input of this.#inputs) {
      if (input === pusher) {
        hadMatch = true;
        continue;
      }

      const constraint: Writable<Constraint> = {};
      for (const key of this.#schema.primaryKey) {
        constraint[key] = change[ChangeIndex.NODE].row[key];
      }
      const fetchResult = input.fetch({
        constraint,
      });

      // `fetch` interleaves 'yield' sentinels for cooperative multitasking.
      // They must be forwarded, not just skipped: this probe runs inside a
      // push, and the sentinel is the source offering the scheduler a breath.
      // They must also not be mistaken for rows -- reading one as a row is
      // what broke this before, since an empty branch that happened to yield
      // looked like a branch holding the row, silently dropping the
      // add/remove and desyncing a downstream `Take`'s push and fetch paths.
      let otherBranchHasRow = false;
      {
        const __pull181 = fetchResult;
        try {
          for (
            let node = __pull181.next();
            node !== undefined;
            node = __pull181.next()
          ) {
            if (node === 'yield') {
              yield node;
              continue;
            }
            otherBranchHasRow = true;
            break;
          }
        } finally {
          __pull181.close();
        }
      }

      if (otherBranchHasRow) {
        // Another branch has the row, so the add/remove is not needed.
        return;
      }
    }

    assert(hadMatch, 'Pusher was not one of the inputs to union-fan-in!');

    // No other branches have the row, so we can push the change.
    yield* this.#output.push(change, this);
  }

  fanOutStartedPushing() {
    assert(
      this.#fanOutPushStarted === false,
      'UnionFanIn: fanOutStartedPushing called while already pushing',
    );
    this.#fanOutPushStarted = true;
  }

  *fanOutDonePushing(fanOutChangeType: ChangeType): Stream<'yield'> {
    assert(
      this.#fanOutPushStarted,
      'UnionFanIn: fanOutDonePushing called without fanOutStartedPushing',
    );
    this.#fanOutPushStarted = false;
    if (this.#inputs.length === 0) {
      return;
    }

    if (this.#accumulatedPushes.length === 0) {
      // It is possible for no forks to pass along the push.
      // E.g., if no filters match in any fork.
      return;
    }

    yield* pushAccumulatedChanges(
      this.#accumulatedPushes,
      this.#output,
      this,
      fanOutChangeType,
      mergeRelationships,
      makeAddEmptyRelationships(this.#schema),
    );
  }

  setOutput(output: Output): void {
    this.#output = output;
  }
}

export function mergeFetches(
  fetches: PullStream<Node | 'yield'>[],
  comparator: (l: Node, r: Node) => number,
): PullStream<Node | 'yield'> {
  return new MergeFetches(fetches, comparator);
}

/**
 * Linear-scan merge of pre-sorted branches, dropping duplicates that compare
 * equal to the last emitted node.
 *
 * The generator this replaces suspended inside its "advance this branch" loop
 * to forward a 'yield'. That point is now `#refill`: the branch owing a
 * replacement for the node just emitted, so a 'yield' can be returned and the
 * merge resumed at the same place.
 */
class MergeFetches extends PullStreamBase<Node | 'yield'> {
  readonly #streams: readonly PullStream<Node | 'yield'>[];
  readonly #comparator: (l: Node, r: Node) => number;
  readonly #current: (Node | null)[];
  #lastEmitted: Node | undefined;
  /** Node selected but not yet emitted: its branch is refilled first. */
  #held: Node | undefined;
  #primeIdx = 0;
  #priming = true;
  #refill: number | undefined;
  #done = false;

  constructor(
    streams: readonly PullStream<Node | 'yield'>[],
    comparator: (l: Node, r: Node) => number,
  ) {
    super();
    this.#streams = streams;
    this.#comparator = comparator;
    this.#current = new Array(streams.length).fill(null);
  }

  /** A Node, 'yield' to forward, or undefined when the branch is spent. */
  #pullOne(idx: number): Node | 'yield' | undefined {
    return this.#streams[idx].next();
  }

  next(): Node | 'yield' | undefined {
    if (this.#done) {
      return undefined;
    }
    for (;;) {
      if (this.#priming) {
        while (this.#primeIdx < this.#streams.length) {
          const v = this.#pullOne(this.#primeIdx);
          if (v === 'yield') {
            return v;
          }
          this.#current[this.#primeIdx] = v === undefined ? null : v;
          this.#primeIdx++;
        }
        this.#priming = false;
      }

      if (this.#refill !== undefined) {
        const idx = this.#refill;
        const v = this.#pullOne(idx);
        if (v === 'yield') {
          return v;
        }
        this.#refill = undefined;
        this.#current[idx] = v === undefined ? null : v;
        // The branch has been advanced; now the held node may be emitted.
        // Order matters: the generator forwarded a branch's 'yield's before
        // emitting the node it had selected from that branch.
        const held = this.#held;
        this.#held = undefined;
        if (held !== undefined) {
          if (
            this.#lastEmitted !== undefined &&
            this.#comparator(this.#lastEmitted, held) === 0
          ) {
            continue;
          }
          this.#lastEmitted = held;
          return held;
        }
        continue;
      }

      let minNode: Node | undefined;
      let minIndex = -1;
      for (let i = 0; i < this.#current.length; i++) {
        const c = this.#current[i];
        if (c === null) {
          continue;
        }
        if (minNode === undefined || this.#comparator(c, minNode) < 0) {
          minNode = c;
          minIndex = i;
        }
      }
      if (minNode === undefined) {
        this.close();
        return undefined;
      }

      // Hold the node and advance its branch first; the duplicate check and
      // the emit both happen once the refill completes.
      this.#held = minNode;
      this.#refill = minIndex;
    }
  }

  close(): void {
    if (this.#done) {
      return;
    }
    this.#done = true;
    this.#held = undefined;
    for (const s of this.#streams) {
      s.close();
    }
  }
}
