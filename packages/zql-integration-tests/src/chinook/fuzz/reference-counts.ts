import {must} from '../../../../shared/src/must.ts';
import {ChangeIndex} from '../../../../zql/src/ivm/change-index.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import type {Change} from '../../../../zql/src/ivm/change.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import type {Input} from '../../../../zql/src/ivm/operator.ts';
import type {SourceSchema} from '../../../../zql/src/ivm/schema.ts';

/**
 * Track every row occurrence streamed to a server consumer, including EXISTS
 * relationships omitted from client views. Compare against a fetch after each
 * mutation: missing REMOVE descendants leave excess counts. Fetch the live
 * pipeline because Cap may retain a different valid subset of EXISTS witnesses
 * than a newly built pipeline would choose.
 */
export class ReferenceCounts {
  readonly counts = new Map<string, number>();
  readonly #input: Input;
  readonly #fetchOnPush: boolean;

  constructor(input: Input, fetchOnPush = false) {
    this.#input = input;
    this.#fetchOnPush = fetchOnPush;
    input.setOutput(this);
    this.#fetch(this.counts);
  }

  #fetch(counts: Map<string, number>) {
    for (const node of this.#input.fetch({})) {
      if (node !== 'yield') {
        this.#node(counts, this.#input.getSchema(), node, 1);
      }
    }
  }

  #row(
    counts: Map<string, number>,
    schema: SourceSchema,
    node: Node,
    delta: number,
  ) {
    const key = JSON.stringify([
      schema.tableName,
      schema.primaryKey.map(col => node.row[col]),
    ]);
    const count = (counts.get(key) ?? 0) + delta;
    if (count === 0) {
      counts.delete(key);
    } else {
      counts.set(key, count);
    }
  }

  #node(
    counts: Map<string, number>,
    schema: SourceSchema,
    node: Node,
    delta: number,
  ) {
    this.#row(counts, schema, node, delta);
    for (const [name, children] of Object.entries(node.relationships)) {
      for (const child of children()) {
        if (child !== 'yield') {
          this.#node(counts, must(schema.relationships[name]), child, delta);
        }
      }
    }
  }

  #change(schema: SourceSchema, change: Change): void {
    switch (change[ChangeIndex.TYPE]) {
      case ChangeType.ADD:
        this.#node(this.counts, schema, change[ChangeIndex.NODE], 1);
        break;
      case ChangeType.REMOVE:
        this.#node(this.counts, schema, change[ChangeIndex.NODE], -1);
        break;
      case ChangeType.EDIT:
        this.#row(this.counts, schema, change[ChangeIndex.OLD_NODE], -1);
        this.#row(this.counts, schema, change[ChangeIndex.NODE], 1);
        break;
      case ChangeType.CHILD: {
        const child = change[ChangeIndex.CHILD_DATA];
        this.#change(
          must(schema.relationships[child.relationshipName]),
          child.change,
        );
        break;
      }
    }
  }

  push(change: Change) {
    this.#change(this.#input.getSchema(), change);
    if (this.#fetchOnPush) {
      // Exercise downstream reads between deliveries of one upstream change.
      // These observations must not replace counts for changes still in flight.
      this.#fetch(new Map());
    }
    return [];
  }

  check(context: string) {
    const fetched = new Map<string, number>();
    this.#fetch(fetched);
    const differences = [...new Set([...this.counts.keys(), ...fetched.keys()])]
      .toSorted()
      .flatMap(key => {
        const actual = this.counts.get(key) ?? 0;
        const expected = fetched.get(key) ?? 0;
        return actual === expected
          ? []
          : [`${key}: expected ${expected}, received ${actual}`];
      });
    if (differences.length) {
      throw new Error(`${context}\n${differences.join('\n')}`);
    }
  }

  destroy() {
    this.#input.destroy();
  }
}
