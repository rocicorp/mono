import {describe, expect, test} from 'vitest';
import type {Row} from '../../../zero-protocol/src/data.ts';
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
  skipYields,
  type FetchRequest,
  type Input,
  type Output,
} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {InputIntersection, InputUnion} from './set-operators.ts';
import {consume, type Stream} from './stream.ts';
import {StripRelationships} from './strip-relationships.ts';

const schema: SourceSchema = {
  tableName: 'assignment',
  columns: {
    id: {type: 'number'},
    value: {type: 'number'},
  },
  primaryKey: ['id'],
  relationships: {},
  compareRows: (left, right) => (left.id as number) - (right.id as number),
  isHidden: false,
  sort: [['id', 'asc']],
  system: 'client',
};

describe('InputUnion', () => {
  test('fetch merges sorted inputs and deduplicates by primary key', () => {
    const left = new MutableInput([
      node({id: 1, value: 10}),
      node({id: 3, value: 30}),
    ]);
    const right = new MutableInput([
      node({id: 2, value: 20}),
      node({id: 3, value: 30}),
    ]);

    const union = new InputUnion([left, right]);

    expect(Array.from(skipYields(union.fetch({})), n => n.row.id)).toEqual([
      1, 2, 3,
    ]);
  });

  test('fetch merges reversed inputs with the reversed comparator', () => {
    const left = new MutableInput([
      node({id: 1, value: 10}),
      node({id: 3, value: 30}),
    ]);
    const right = new MutableInput([
      node({id: 2, value: 20}),
      node({id: 3, value: 30}),
      node({id: 4, value: 40}),
    ]);

    const union = new InputUnion([left, right]);

    expect(
      Array.from(skipYields(union.fetch({reverse: true})), n => n.row.id),
    ).toEqual([4, 3, 2, 1]);
  });

  test('push emits one add, remove, or edit when multiple branches match', () => {
    const left = new MutableInput([]);
    const right = new MutableInput([]);
    const union = new InputUnion([left, right]);
    const sink = new RecordingOutput();
    union.setOutput(sink);

    left.rows = [node({id: 1, value: 10})];
    left.push(makeAddChange(node({id: 1, value: 10})));
    right.rows = [node({id: 1, value: 10})];
    right.push(makeAddChange(node({id: 1, value: 10})));

    left.rows = [node({id: 1, value: 11})];
    left.push(
      makeEditChange(node({id: 1, value: 11}), node({id: 1, value: 10})),
    );
    right.rows = [node({id: 1, value: 11})];
    right.push(
      makeEditChange(node({id: 1, value: 11}), node({id: 1, value: 10})),
    );

    left.rows = [];
    left.push(makeRemoveChange(node({id: 1, value: 11})));
    right.rows = [];
    right.push(makeRemoveChange(node({id: 1, value: 11})));

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.ADD,
      ChangeType.EDIT,
      ChangeType.REMOVE,
    ]);
  });

  test('push emits primary key edits as remove and add', () => {
    const left = new MutableInput([node({id: 1, value: 10})]);
    const right = new MutableInput([]);
    const union = new InputUnion([left, right]);
    const sink = new RecordingOutput();
    union.setOutput(sink);

    left.rows = [node({id: 2, value: 10})];
    left.push(
      makeEditChange(node({id: 2, value: 10}), node({id: 1, value: 10})),
    );

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.REMOVE,
      ChangeType.ADD,
    ]);
    expect(sink.changes.map(change => change[ChangeIndex.NODE].row.id)).toEqual(
      [1, 2],
    );
  });

  test('push emits an edit when representative ownership moves earlier', () => {
    const left = new MutableInput([]);
    const right = new MutableInput([node({id: 1, value: 20})]);
    const union = new InputUnion([left, right]);
    const sink = new RecordingOutput();
    union.setOutput(sink);

    left.rows = [node({id: 1, value: 10})];
    left.push(makeAddChange(node({id: 1, value: 10})));

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.EDIT,
    ]);
    const [change] = sink.changes;
    expect(change).toBeDefined();
    if (!change) {
      throw new Error('Expected change');
    }
    expect(change[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
    if (change[ChangeIndex.TYPE] !== ChangeType.EDIT) {
      throw new Error('Expected edit');
    }
    expect(change[ChangeIndex.NODE].row).toEqual({
      id: 1,
      value: 10,
    });
    expect(change[ChangeIndex.OLD_NODE].row).toEqual({
      id: 1,
      value: 20,
    });
  });

  test('push emits an edit when representative ownership moves later', () => {
    const left = new MutableInput([node({id: 1, value: 10})]);
    const right = new MutableInput([node({id: 1, value: 20})]);
    const union = new InputUnion([left, right]);
    const sink = new RecordingOutput();
    union.setOutput(sink);

    left.rows = [];
    left.push(makeRemoveChange(node({id: 1, value: 10})));

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.EDIT,
    ]);
    const [change] = sink.changes;
    expect(change).toBeDefined();
    if (!change) {
      throw new Error('Expected change');
    }
    expect(change[ChangeIndex.TYPE]).toBe(ChangeType.EDIT);
    if (change[ChangeIndex.TYPE] !== ChangeType.EDIT) {
      throw new Error('Expected edit');
    }
    expect(change[ChangeIndex.NODE].row).toEqual({
      id: 1,
      value: 20,
    });
    expect(change[ChangeIndex.OLD_NODE].row).toEqual({
      id: 1,
      value: 10,
    });
  });

  test('push forwards child changes from the earliest matching branch only', () => {
    const left = new MutableInput([node({id: 1, value: 10})]);
    const right = new MutableInput([node({id: 1, value: 10})]);
    const union = new InputUnion([left, right]);
    const sink = new RecordingOutput();
    union.setOutput(sink);

    left.push(childChange(node({id: 1, value: 10})));
    right.push(childChange(node({id: 1, value: 10})));

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.CHILD,
    ]);
  });

  test('requires relationship-free branch streams', () => {
    const related = new MutableInput([], {
      ...schema,
      relationships: {children: schema},
    });

    expect(() => new InputUnion([related])).toThrow(
      'input union requires inputs without relationships',
    );
  });
});

describe('StripRelationships', () => {
  test('removes relationship schema and fetched relationship payloads', () => {
    const input = new MutableInput(
      [
        node(
          {id: 1, value: 10},
          {
            children: () => [node({id: 99, value: 99})],
          },
        ),
      ],
      {
        ...schema,
        relationships: {children: schema},
      },
    );

    const stripped = new StripRelationships(input);

    expect(stripped.getSchema().relationships).toEqual({});
    expect(
      Array.from(skipYields(stripped.fetch({})), node => node.relationships),
    ).toEqual([{}]);
  });

  test('ignores relationship-only pushes', () => {
    const input = new MutableInput([], {
      ...schema,
      relationships: {children: schema},
    });
    const stripped = new StripRelationships(input);
    const sink = new RecordingOutput();
    stripped.setOutput(sink);

    input.push(childChange(node({id: 1, value: 10})));

    expect(sink.changes).toEqual([]);
  });
});

describe('InputIntersection', () => {
  test('fetch keeps rows whose key appears in every input', () => {
    const left = new MutableInput([
      node({id: 1, value: 10}),
      node({id: 2, value: 20}),
    ]);
    const right = new MutableInput([
      node({id: 2, value: 200}),
      node({id: 3, value: 300}),
    ]);

    const intersection = new InputIntersection([left, right], ['id']);

    expect(Array.from(skipYields(intersection.fetch({})), n => n.row)).toEqual([
      {id: 2, value: 20},
    ]);
  });

  test('fetch intersects different child schemas by mapped key', () => {
    const access = new MutableInput(
      [
        node({assignment_id: 1, teacher_id: 10}),
        node({assignment_id: 2, teacher_id: 10}),
      ],
      childSchema('teacher_assignment_access', ['assignment_id', 'teacher_id']),
    );
    const membership = new MutableInput(
      [
        node({owner_assignment_id: 2, class_id: 20}),
        node({owner_assignment_id: 3, class_id: 20}),
      ],
      childSchema('assignment_to_class', ['owner_assignment_id', 'class_id']),
    );

    const intersection = new InputIntersection(
      [access, membership],
      ['assignment_id'],
      [['assignment_id'], ['owner_assignment_id']],
    );

    expect(Array.from(skipYields(intersection.fetch({})), n => n.row)).toEqual([
      {assignment_id: 2, teacher_id: 10},
    ]);
  });

  test('fetch deduplicates rows by intersection key', () => {
    const left = new MutableInput([
      node({id: 1, value: 10}),
      node({id: 1, value: 11}),
      node({id: 2, value: 20}),
    ]);
    const right = new MutableInput([node({id: 1, value: 100})]);

    const intersection = new InputIntersection([left, right], ['id']);

    expect(Array.from(skipYields(intersection.fetch({})), n => n.row)).toEqual([
      {id: 1, value: 10},
    ]);
  });

  test('fetch streams the representative branch after reading other key sets', () => {
    const events: string[] = [];
    const left = new LoggingInput(
      'first',
      [node({id: 1, value: 10}), node({id: 2, value: 20})],
      events,
    );
    const right = new LoggingInput(
      'other',
      [node({id: 2, value: 200})],
      events,
    );
    const intersection = new InputIntersection([left, right], ['id']);

    const iter = intersection.fetch({})[Symbol.iterator]();
    const result = iter.next();

    expect(result.done).toBe(false);
    expect(result.value).toEqual(node({id: 2, value: 20}));
    expect(events).toEqual(['other:start', 'other:end', 'first:start']);
  });

  test('push emits when a key enters or leaves the intersection', () => {
    const left = new MutableInput([node({id: 1, value: 10})]);
    const right = new MutableInput([]);
    const intersection = new InputIntersection([left, right], ['id']);
    const sink = new RecordingOutput();
    intersection.setOutput(sink);

    right.rows = [node({id: 1, value: 100})];
    right.push(makeAddChange(node({id: 1, value: 100})));

    right.rows = [];
    right.push(makeRemoveChange(node({id: 1, value: 100})));

    expect(sink.changes).toHaveLength(2);
    expect(sink.changes[0][ChangeIndex.TYPE]).toBe(ChangeType.ADD);
    expect(sink.changes[0][ChangeIndex.NODE].row).toEqual({
      id: 1,
      value: 10,
    });
    expect(sink.changes[1][ChangeIndex.TYPE]).toBe(ChangeType.REMOVE);
    expect(sink.changes[1][ChangeIndex.NODE].row).toEqual({
      id: 1,
      value: 10,
    });
  });

  test('push maps key constraints across different child schemas', () => {
    const access = new MutableInput(
      [node({assignment_id: 1, teacher_id: 10})],
      childSchema('teacher_assignment_access', ['assignment_id', 'teacher_id']),
    );
    const membership = new MutableInput(
      [],
      childSchema('assignment_to_class', ['owner_assignment_id', 'class_id']),
    );
    const intersection = new InputIntersection(
      [access, membership],
      ['assignment_id'],
      [['assignment_id'], ['owner_assignment_id']],
    );
    const sink = new RecordingOutput();
    intersection.setOutput(sink);

    membership.rows = [node({owner_assignment_id: 1, class_id: 20})];
    membership.push(
      makeAddChange(node({owner_assignment_id: 1, class_id: 20})),
    );

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.ADD,
    ]);
    expect(sink.changes[0][ChangeIndex.NODE].row).toEqual({
      assignment_id: 1,
      teacher_id: 10,
    });
  });

  test('push removes and edits non-primary branches by mapped key', () => {
    const access = new MutableInput(
      [
        node({assignment_id: 1, teacher_id: 10}),
        node({assignment_id: 2, teacher_id: 10}),
      ],
      childSchema('teacher_assignment_access', ['assignment_id', 'teacher_id']),
    );
    const membership = new MutableInput(
      [node({owner_assignment_id: 1, class_id: 20})],
      childSchema('assignment_to_class', ['owner_assignment_id', 'class_id']),
    );
    const intersection = new InputIntersection(
      [access, membership],
      ['assignment_id'],
      [['assignment_id'], ['owner_assignment_id']],
    );
    const sink = new RecordingOutput();
    intersection.setOutput(sink);

    membership.rows = [node({owner_assignment_id: 2, class_id: 20})];
    membership.push(
      makeEditChange(
        node({owner_assignment_id: 2, class_id: 20}),
        node({owner_assignment_id: 1, class_id: 20}),
      ),
    );

    membership.rows = [];
    membership.push(
      makeRemoveChange(node({owner_assignment_id: 2, class_id: 20})),
    );

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.REMOVE,
      ChangeType.ADD,
      ChangeType.REMOVE,
    ]);
    expect(sink.changes.map(change => change[ChangeIndex.NODE].row)).toEqual([
      {assignment_id: 1, teacher_id: 10},
      {assignment_id: 2, teacher_id: 10},
      {assignment_id: 2, teacher_id: 10},
    ]);
  });

  test('push emits remove and add when a non-primary branch moves keys', () => {
    const left = new MutableInput([
      node({id: 1, value: 10}),
      node({id: 2, value: 20}),
    ]);
    const right = new MutableInput([node({id: 1, value: 100})]);
    const intersection = new InputIntersection([left, right], ['id']);
    const sink = new RecordingOutput();
    intersection.setOutput(sink);

    right.rows = [node({id: 2, value: 100})];
    right.push(
      makeEditChange(node({id: 2, value: 100}), node({id: 1, value: 100})),
    );

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.REMOVE,
      ChangeType.ADD,
    ]);
    expect(sink.changes.map(change => change[ChangeIndex.NODE].row)).toEqual([
      {id: 1, value: 10},
      {id: 2, value: 20},
    ]);
  });

  test('push forwards child changes from the representative branch only', () => {
    const left = new MutableInput([node({id: 1, value: 10})]);
    const right = new MutableInput([node({id: 1, value: 100})]);
    const intersection = new InputIntersection([left, right], ['id']);
    const sink = new RecordingOutput();
    intersection.setOutput(sink);

    right.push(childChange(node({id: 1, value: 100})));
    left.push(childChange(node({id: 1, value: 10})));

    expect(sink.changes.map(change => change[ChangeIndex.TYPE])).toEqual([
      ChangeType.CHILD,
    ]);
    expect(sink.changes[0][ChangeIndex.NODE].row).toEqual({
      id: 1,
      value: 10,
    });
  });
});

class MutableInput implements Input {
  rows: Node[];
  readonly #schema: SourceSchema;
  #output: Output | undefined;

  constructor(rows: Node[], sourceSchema: SourceSchema = schema) {
    this.rows = rows;
    this.#schema = sourceSchema;
  }

  setOutput(output: Output): void {
    this.#output = output;
  }

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    const rows = req.reverse ? this.rows.toReversed() : this.rows;
    for (const row of rows) {
      if (matchesConstraint(row.row, req.constraint)) {
        yield row;
      }
    }
  }

  push(change: Change): void {
    consume(this.#output?.push(change, this) ?? []);
  }

  destroy(): void {}
}

class LoggingInput extends MutableInput {
  readonly #label: string;
  readonly #events: string[];

  constructor(label: string, rows: Node[], events: string[]) {
    super(rows);
    this.#label = label;
    this.#events = events;
  }

  override *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    this.#events.push(`${this.#label}:start`);
    yield* super.fetch(req);
    this.#events.push(`${this.#label}:end`);
  }
}

class RecordingOutput implements Output {
  readonly changes: Change[] = [];

  *push(change: Change): Stream<'yield'> {
    this.changes.push(change);
  }
}

function node(row: Row, relationships: Node['relationships'] = {}): Node {
  return {row, relationships};
}

function childChange(parent: Node): Change {
  return makeChildChange(parent, {
    relationshipName: 'children',
    change: makeAddChange(node({id: 99, value: 99})),
  });
}

function matchesConstraint(
  row: Row,
  constraint: FetchRequest['constraint'],
): boolean {
  if (!constraint) {
    return true;
  }
  return Object.entries(constraint).every(([key, value]) => row[key] === value);
}

function childSchema(
  tableName: string,
  primaryKey: readonly [string, ...string[]],
): SourceSchema {
  return {
    tableName,
    columns: Object.fromEntries(
      primaryKey.map(column => [column, {type: 'number'}]),
    ),
    primaryKey,
    relationships: {},
    compareRows: (left, right) =>
      (left[primaryKey[0]] as number) - (right[primaryKey[0]] as number),
    isHidden: false,
    sort: primaryKey.map(column => [column, 'asc']),
    system: 'client',
  };
}
