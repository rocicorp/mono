import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {TableSchema} from '../../../zero-types/src/schema.ts';
import type {DebugDelegate} from '../builder/debug-delegate.ts';
import {ChangeType} from './change-type.ts';
import type {Input} from './operator.ts';
import type {Stream} from './stream.ts';

export type SourceChangeAdd = [type: ChangeType.ADD, row: Row, extra: null];
export type SourceChangeRemove = [
  type: ChangeType.REMOVE,
  row: Row,
  extra: null,
];
export type SourceChangeEdit = [type: ChangeType.EDIT, row: Row, oldRow: Row];

export type SourceChange =
  | SourceChangeAdd
  | SourceChangeRemove
  | SourceChangeEdit;

export function makeSourceChangeAdd(row: Row): SourceChangeAdd {
  return [ChangeType.ADD, row, null];
}

export function makeSourceChangeRemove(row: Row): SourceChangeRemove {
  return [ChangeType.REMOVE, row, null];
}

export function makeSourceChangeEdit(row: Row, oldRow: Row): SourceChangeEdit {
  return [ChangeType.EDIT, row, oldRow];
}

/**
 * Listener notified when the source begins and ends pushing a single
 * logical change to all of its connections. Used by `SourceTxnCoordinator`
 * to drive the `MultiSourceUnionFanIn` accumulation window without a
 * `UnionFanOut` directly above the union.
 *
 * `endPush` is invoked once for each `beginPush`. For an `EDIT` that the
 * source splits into REMOVE+ADD, two begin/end pairs fire (one per half).
 *
 * `endPush` returns a stream so it can yield while the underlying union
 * fan-in flushes accumulated pushes downstream.
 */
export interface SourceTxnListener {
  beginPush(): void;
  endPush(changeType: ChangeType): Stream<'yield'>;
}

/**
 * A source is an input that serves as the root data source of the pipeline.
 * Sources have multiple outputs. To add an output, call `connect()`, then
 * hook yourself up to the returned Connector, like:
 *
 * ```ts
 * class MyOperator implements Output {
 *   constructor(input: Input) {
 *     input.setOutput(this);
 *   }
 *
 *   push(change: Change): void {
 *     // Handle change
 *   }
 * }
 *
 * const connection = source.connect(ordering);
 * const myOperator = new MyOperator(connection);
 * ```
 */
export interface Source {
  get tableSchema(): TableSchema;
  /**
   * Creates an input that an operator can connect to. To free resources used
   * by connection, downstream operators call `destroy()` on the returned
   * input.
   *
   * @param sort The ordering of the rows. Source must return rows in this
   * order.  Must include all primary keys of the table.
   * @param filters Filters to apply to the source.
   * @param splitEditKeys If an edit change modifies the values of any of the
   *   keys in splitEditKeys, the source should split the edit change into
   *   a remove of the old row followed by an add of the new row.
   */
  connect(
    sort: Ordering | undefined,
    filters?: Condition,
    splitEditKeys?: Set<string>,
    debug?: DebugDelegate,
  ): SourceInput;

  /**
   * Pushes a change into the source and into all connected outputs.
   *
   * The returned stream can yield 'yield' to yield control to the caller
   * for purposes of responsiveness.
   *
   * Once the stream is exhausted, the change will have been pushed into all
   * connected inputs and committed to the source.
   */
  push(change: SourceChange): Stream<'yield'>;

  /**
   * Pushes a change into the source.
   *
   * Iterating the returned stream will push the change into one connected input
   * at a time, yielding `undefined` between each, and yielding `'yield'` to
   * yield control to the caller for purposes of responsiveness.
   *
   * Once the stream is exhausted, the change will have been pushed
   * into all connected inputs and committed to the source.
   */
  genPush(change: SourceChange): Stream<'yield' | undefined>;

  /**
   * Register a listener that is notified at the start and end of each
   * logical push (one begin/end pair per `genPushAndWrite` invocation,
   * so split-edit pushes fire two pairs). Returns an unsubscribe function.
   */
  addTxnListener(listener: SourceTxnListener): () => void;
}

export interface SourceInput extends Input {
  readonly fullyAppliedFilters: boolean;
}
