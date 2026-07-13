import type {JSONValue} from '../../../shared/src/json.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Change} from './change.ts';
import type {Constraint} from './constraint.ts';
import type {Node} from './data.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

export {skipYields} from './skip-yields.ts';

/**
 * Input to an operator.
 */
export interface InputBase {
  /** The schema of the data this input returns. */
  getSchema(): SourceSchema;

  /**
   * Completely destroy the input. Destroying an input
   * causes it to call destroy on its upstreams, fully
   * cleaning up a pipeline.
   */
  destroy(): void;
}

export interface Input extends InputBase {
  /** Tell the input where to send its output. */
  setOutput(output: Output): void;

  /**
   * Fetch data. May modify the data in place.
   * Returns nodes sorted in order of `SourceSchema.compareRows`.
   *
   * The stream may contain 'yield' to yield control to the caller for purposes
   * of responsiveness.
   *
   * Contract:
   * - During fetch: If an input yields 'yield', 'yield' must be yielded to the
   * caller of fetch immediately.
   * - During push: If a fetch to an input consumed by the push logic yields
   * 'yield', it must be yielded to the caller of push immediately.
   */
  fetch(req: FetchRequest): Stream<Node | 'yield'>;

  /**
   * Notifies this input that the rows matching `constraint` are permanently
   * leaving the view of the pipeline above it (their partition is being
   * discarded, e.g. because the parent row of a partitioned Take/Cap was
   * removed). Operators that maintain per-partition state (Take, Cap) delete
   * the state for that partition; Joins recursively clean up child-pipeline
   * partitions keyed by the departing rows; other operators forward the call
   * to their upstream input(s).
   *
   * Optional: an input without this method is treated as a leaf with no
   * per-partition state (see {@link cleanupPartition}).
   *
   * The stream may contain 'yield' with the same contract as `fetch`/`push`.
   */
  cleanupPartition?: ((constraint: Constraint) => Stream<'yield'>) | undefined;

  /**
   * Whether this input's upstream chain contains per-partition state that
   * {@link cleanupPartition} could delete. Used to skip cleanup work
   * (including the fetches needed to drive it) for pipelines without
   * partitioned operators.
   *
   * Optional: an input without this method is treated as having no
   * per-partition state (see {@link inputNeedsPartitionCleanup}).
   */
  needsPartitionCleanup?: (() => boolean) | undefined;
}

/**
 * Returns whether `input`'s upstream chain contains per-partition state that
 * {@link Input.cleanupPartition} could delete. Inputs that do not implement
 * `needsPartitionCleanup` (e.g. source connections or external custom
 * sources) are leaves with no per-partition state.
 */
export function inputNeedsPartitionCleanup(input: Input): boolean {
  return input.needsPartitionCleanup?.() ?? false;
}

/**
 * Forwards {@link Input.cleanupPartition} to `input` if it implements it.
 */
export function* cleanupPartition(
  input: Input,
  constraint: Constraint,
): Stream<'yield'> {
  if (input.cleanupPartition) {
    yield* input.cleanupPartition(constraint);
  }
}

/**
 * A single multi-row IN clause: a non-empty list of Constraints all
 * sharing the same column shape. Sources treat it as
 * `(col_a, col_b, …) IN VALUES (…)`.
 *
 * Caller invariants (sources rely on these — they are not re-checked):
 * - **Unique entries.** TableSource gets set semantics for free from SQL
 *   `IN`; MemorySource fans sub-fetches out per entry, so duplicates
 *   would yield the same row N times. FlippedJoin groups by canonical
 *   parent-key (`flipped-join.ts:#fetchBatched`) before emitting.
 * - **Key-compatible with `req.constraint`.** Entries that contradict
 *   `constraint` should be dropped upstream. FlippedJoin filters
 *   incompatible children via `constraintsAreCompatible` before adding
 *   them to a batch.
 */
export type MultiConstraint = readonly Constraint[];

export type FetchRequest = {
  readonly constraint?: Constraint | undefined;

  /**
   * List of multi-row IN clauses, all ANDed together (and ANDed with
   * `constraint` if both are provided). Each entry is a `MultiConstraint`
   * over its own set of columns.
   *
   * Used by FlippedJoin to push child→parent fetches into a single
   * batched statement, and to AND together constraints contributed by
   * chained FlippedJoins (e.g. `assigneeID IN (…) AND creatorID IN (…)`).
   */
  readonly multiConstraints?: readonly MultiConstraint[] | undefined;

  /** If supplied, `start.row` must have previously been output by fetch or push. */
  readonly start?: Start | undefined;

  /** Whether to fetch in reverse order of the SourceSchema's sort. */
  readonly reverse?: boolean | undefined;
};

export type Start = {
  readonly row: Row;
  readonly basis: 'at' | 'after';
};

/**
 * An output for an operator. Typically another Operator but can also be
 * the code running the pipeline.
 */
export interface Output {
  /**
   * Push incremental changes to data previously received with fetch().
   * Consumers must apply all pushed changes or incremental result will
   * be incorrect.
   * Callers must maintain some invariants for correct operation:
   * - Only add rows which do not already exist (by deep equality).
   * - Only remove rows which do exist (by deep equality).
   * Implmentation can yield 'yield' to yield control to the caller for purposes
   * of responsiveness.
   * Yield contract:
   * - During a push: If a push call to an output yields 'yield', it must be
   * yielded to the caller of push immediately.
   */
  push(change: Change, pusher: InputBase): Stream<'yield'>;
}

/**
 * An implementation of Output that throws if pushed to. It is used as the
 * initial value for for an operator's output before it is set.
 */
export const throwOutput: Output = {
  push(_change: Change): Stream<'yield'> {
    throw new Error('Output not set');
  },
};

/**
 * Operators are arranged into pipelines.
 * They are stateful.
 * Each operator is an input to the next operator in the chain and an output
 * to the previous.
 */
export interface Operator extends Input, Output {}

/**
 * Operators get access to storage that they can store their internal
 * state in.
 */
export interface Storage {
  set(key: string, value: JSONValue): void;
  get(key: string, def?: JSONValue): JSONValue | undefined;
  /**
   * If options is not specified, defaults to scanning all entries.
   */
  scan(options?: {prefix: string}): Stream<[string, JSONValue]>;
  del(key: string): void;
  /**
   * Deletes all entries. Called when the operator owning this storage is
   * destroyed. Optional: {@link clearStorage} falls back to scan + del.
   */
  truncate?: (() => void) | undefined;
}

/**
 * Deletes all entries in `storage`, using {@link Storage.truncate} when
 * available.
 */
export function clearStorage(storage: {
  scan(options?: {prefix: string}): Stream<[key: string, value: unknown]>;
  del(key: string): void;
  truncate?: (() => void) | undefined;
}): void {
  if (storage.truncate) {
    storage.truncate();
    return;
  }
  const keys: string[] = [];
  for (const [key] of storage.scan()) {
    keys.push(key);
  }
  for (const key of keys) {
    storage.del(key);
  }
}
