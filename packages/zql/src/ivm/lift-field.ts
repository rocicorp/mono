import {unreachable} from '../../../shared/src/asserts.ts';
import {emptyArray} from '../../../shared/src/sentinels.ts';
import {
  aggregateValue,
  type Aggregate,
} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {ChangeIndex} from './change-index.ts';
import {ChangeType} from './change-type.ts';
import {
  makeAddChange,
  makeEditChange,
  makeRemoveChange,
  type Change,
} from './change.ts';
import type {Node} from './data.ts';
import {
  throwOutput,
  type FetchRequest,
  type Input,
  type InputBase,
  type Operator,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';

/**
 * The synthetic column {@link LiftField} writes the destination field into.
 * Internal to the junction-aggregate sub-pipeline
 * (junction → Join(dest) → LiftField → Aggregate); the Aggregate reads it as an
 * ordinary flat field. Space-prefixed so it can never collide with a real
 * column (identifiers don't start with a space).
 */
export const LIFTED_FIELD_COLUMN = ' lifted';

/**
 * Lifts a field from a singular child relationship onto each row as a synthetic
 * column ({@link LIFTED_FIELD_COLUMN}).
 *
 * Used for junction (many-to-many) aggregates: the value being aggregated (e.g.
 * `label.points`) lives one hop past the junction (`issueLabel`), so the field
 * isn't on the rows the {@link Aggregate} operator would see. LiftField copies
 * it onto the junction row, turning a two-hop aggregate into the flat
 * single-hop that Aggregate already handles (count/sum/avg/min/max).
 *
 * Change translation:
 *  - add/remove/edit of the junction row → same, with the column filled in;
 *  - a change to the *destination* arrives as a CHILD change on the junction row
 *    and becomes an EDIT of the lifted column (add/remove of the destination map
 *    to value↔null). A change below the destination (e.g. to the row an
 *    `EXISTS` filter on it checks, when existence doesn't flip) leaves the
 *    destination row, and so the lifted value, as it was.
 *
 * The output rows carry no relationships.
 *
 * For an aggregate over a JSON path, `jsonAggregate` (whose `field` is the
 * destination's json column) makes it lift the extracted leaf, gated as
 * `aggregateValue` does, instead of the whole document; the Aggregate then
 * reads the lifted column as a plain field.
 */
export class LiftField implements Operator {
  readonly #input: Input;
  readonly #sourceRelationship: string;
  readonly #field: string;
  readonly #jsonAggregate: Aggregate | undefined;
  readonly #schema: SourceSchema;
  #output: Output = throwOutput;

  constructor(
    input: Input,
    sourceRelationship: string,
    field: string,
    jsonAggregate?: Aggregate,
  ) {
    this.#input = input;
    this.#sourceRelationship = sourceRelationship;
    this.#field = field;
    this.#jsonAggregate = jsonAggregate;
    input.setOutput(this);

    const inputSchema = input.getSchema();
    const destSchema = inputSchema.relationships[sourceRelationship];
    const fieldSchema = destSchema?.columns[field];
    this.#schema = {
      ...inputSchema,
      columns: {
        ...inputSchema.columns,
        // Nullable: a junction row may (transiently) have no destination.
        [LIFTED_FIELD_COLUMN]:
          jsonAggregate !== undefined
            ? // A JSON leaf: a number for sum/avg, a number or a string for
              // min/max.
              jsonAggregate.fn === 'min' || jsonAggregate.fn === 'max'
              ? {type: 'json', optional: true}
              : {type: 'number', optional: true}
            : fieldSchema
              ? {...fieldSchema, optional: true}
              : {type: 'number', optional: true},
      },
      relationships: {},
    };
  }

  /** The value to lift from a destination row. */
  #read(row: Row): Value {
    return (
      (this.#jsonAggregate === undefined
        ? row[this.#field]
        : (aggregateValue(this.#jsonAggregate, row) as Value)) ?? null
    );
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  destroy(): void {
    this.#input.destroy();
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    // Every junction row passes through with its destination's field copied
    // onto it as LIFTED_FIELD_COLUMN (see #lift); yields pass through as is.
    for (const node of this.#input.fetch(req)) {
      yield node === 'yield' ? node : this.#lift(node);
    }
  }

  /**
   * Phase 2 of the two-phase push. Nothing here is bounded, so the signal is
   * only forwarded for bounded operators downstream.
   */
  reconcile(_pusher: InputBase): Stream<'yield'> {
    return this.#output.reconcile?.(this) ?? emptyArray;
  }

  push(change: Change): Stream<'yield'> {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        return this.#output.push(
          makeAddChange(this.#lift(change[ChangeIndex.NODE])),
          this,
        );
      case ChangeType.REMOVE:
        return this.#output.push(
          makeRemoveChange(this.#lift(change[ChangeIndex.NODE])),
          this,
        );
      case ChangeType.EDIT:
        return this.#output.push(
          makeEditChange(
            this.#lift(change[ChangeIndex.NODE]),
            this.#lift(change[ChangeIndex.OLD_NODE]),
          ),
          this,
        );
      case ChangeType.CHILD: {
        const childData = change[ChangeIndex.CHILD_DATA];
        if (childData.relationshipName !== this.#sourceRelationship) {
          return emptyArray; // a relationship we don't lift from
        }
        // The destination changed → an edit of the lifted column, with both
        // values read off the destination's change rather than re-fetched.
        const values = this.#valuesFromChildChange(childData.change);
        if (values === undefined) {
          return emptyArray;
        }
        const [oldValue, newValue] = values;
        const {row} = change[ChangeIndex.NODE];
        return this.#output.push(
          makeEditChange(
            {row: {...row, [LIFTED_FIELD_COLUMN]: newValue}, relationships: {}},
            {row: {...row, [LIFTED_FIELD_COLUMN]: oldValue}, relationships: {}},
          ),
          this,
        );
      }
      default:
        return emptyArray;
    }
  }

  /** `node`'s row with the destination's field lifted onto it. */
  #lift(node: Node): Node {
    return {
      row: {...node.row, [LIFTED_FIELD_COLUMN]: this.#liftValue(node)},
      relationships: {},
    };
  }

  /**
   * The destination field's value for `node`, or null when there is none. The
   * destination is a single row looked up by its key, so the read is not
   * worth yielding in the middle of.
   */
  #liftValue(node: Node): Value {
    const rel = node.relationships[this.#sourceRelationship];
    if (rel === undefined) {
      return null;
    }
    let value: Value = null;
    for (const child of rel()) {
      if (child !== 'yield') {
        value = this.#read(child.row);
      }
    }
    return value;
  }

  /**
   * The lifted value before and after a change to the destination, or
   * undefined when the destination row itself didn't change (a CHILD change
   * from below it).
   */
  #valuesFromChildChange(childChange: Change): [Value, Value] | undefined {
    switch (childChange[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        return [null, this.#read(childChange[ChangeIndex.NODE].row)];
      case ChangeType.REMOVE:
        return [this.#read(childChange[ChangeIndex.NODE].row), null];
      case ChangeType.EDIT:
        return [
          this.#read(childChange[ChangeIndex.OLD_NODE].row),
          this.#read(childChange[ChangeIndex.NODE].row),
        ];
      case ChangeType.CHILD:
        return undefined;
      default:
        unreachable(childChange);
    }
  }
}
