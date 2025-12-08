import {describe, expect, test} from 'vitest';
import type {FetchRequest, Input, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import {compareValues, type Node} from './data.ts';
import {FilterEnd, FilterStart} from './filter-operators.ts';
import {Skip} from './skip.ts';
import {Take} from './take.ts';
import {Snitch} from './snitch.ts';
import {Catch} from './catch.ts';
import {Join} from './join.ts';
import {UnionFanIn} from './union-fan-in.ts';
import {UnionFanOut} from './union-fan-out.ts';
import {FlippedJoin} from './flipped-join.ts';
import {Exists} from './exists.ts';
import {MemoryStorage} from './memory-storage.ts';

const YIELD_SOURCE_SCHEMA_BASE: SourceSchema = {
  tableName: 'table1',
  primaryKey: ['id'],
  columns: {id: {type: 'string'}},
  relationships: {},
  system: 'client',
  sort: [['id', 'asc']],
  compareRows: (a, b) => compareValues(a.id, b.id),
  isHidden: false,
};
class YieldSource implements Input {
  #schema: SourceSchema;

  constructor(schema: Partial<SourceSchema> = {}) {
    this.#schema = {
      ...YIELD_SOURCE_SCHEMA_BASE,
      ...schema,
    };
  }

  setOutput(_: Output): void {}

  getSchema(): SourceSchema {
    return this.#schema;
  }

  *fetch(_req: FetchRequest): Stream<Node | 'yield'> {
    yield 'yield';
    yield {row: {id: '1'}, relationships: {}};
    yield 'yield';
    yield {row: {id: '2'}, relationships: {}};
  }

  destroy(): void {}
}

describe('Yield Propagation', () => {
  test('FilterStart/End propagates yield', () => {
    const source = new YieldSource();
    const start = new FilterStart(source);
    const end = new FilterEnd(start, start);
    const catchOp = new Catch(end);
    expect(catchOp.fetch({})).toMatchInlineSnapshot(`
      [
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "1",
          },
        },
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "2",
          },
        },
      ]
    `);
  });

  test('Skip propagates yield', () => {
    const source = new YieldSource();
    const skip = new Skip(source, {row: {id: ''}, exclusive: false});
    const catchOp = new Catch(skip);
    expect(catchOp.fetch({})).toMatchInlineSnapshot(`
      [
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "1",
          },
        },
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "2",
          },
        },
      ]
    `);
  });

  test('Take propagates yield', () => {
    const source = new YieldSource();
    const take = new Take(source, new MemoryStorage(), 10);
    const catchOp = new Catch(take);
    expect(catchOp.fetch({})).toMatchInlineSnapshot(`
      [
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "1",
          },
        },
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "2",
          },
        },
      ]
    `);
  });

  test('Snitch propagates yield', () => {
    const source = new YieldSource();
    const snitch = new Snitch(source, 'snitch');
    const catchOp = new Catch(snitch);
    expect(catchOp.fetch({})).toMatchInlineSnapshot(`
      [
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "1",
          },
        },
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "2",
          },
        },
      ]
    `);
  });

  test('UnionFanIn propagates yield', () => {
    const source1 = new YieldSource();
    const source2 = new YieldSource();
    const fanOut = new UnionFanOut(new YieldSource());
    const ufi = new UnionFanIn(fanOut, [source1, source2]);
    const catchOp = new Catch(ufi);
    expect(catchOp.fetch({})).toMatchInlineSnapshot(`
      [
        "yield",
        "yield",
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "1",
          },
        },
        "yield",
        {
          "relationships": {},
          "row": {
            "id": "2",
          },
        },
      ]
    `);
  });

  test('Join propagates parent and child yields', () => {
    const parent = new YieldSource({tableName: 'parent'});
    const child = new YieldSource({tableName: 'child'});
    const join = new Join({
      parent,
      child,
      parentKey: ['id'],
      childKey: ['id'],
      relationshipName: 'child',
      hidden: false,
      system: 'client',
    });
    const catchOp = new Catch(join);
    expect(catchOp.fetch({})).toMatchInlineSnapshot(`
      [
        "yield",
        {
          "relationships": {
            "child": [
              "yield",
              {
                "relationships": {},
                "row": {
                  "id": "1",
                },
              },
              "yield",
              {
                "relationships": {},
                "row": {
                  "id": "2",
                },
              },
            ],
          },
          "row": {
            "id": "1",
          },
        },
        "yield",
        {
          "relationships": {
            "child": [
              "yield",
              {
                "relationships": {},
                "row": {
                  "id": "1",
                },
              },
              "yield",
              {
                "relationships": {},
                "row": {
                  "id": "2",
                },
              },
            ],
          },
          "row": {
            "id": "2",
          },
        },
      ]
    `);
  });

  test('FlippedJoin propagates parent and child yield (all end up at top level)', () => {
    const parent = new YieldSource({tableName: 'parent'});
    const child = new YieldSource({tableName: 'child'});
    const flippedJoin = new FlippedJoin({
      parent,
      child,
      parentKey: ['id'],
      childKey: ['id'],
      relationshipName: 'child',
      hidden: false,
      system: 'client',
    });
    const catchOp = new Catch(flippedJoin);
    expect(catchOp.fetch({})).toMatchInlineSnapshot(`
      [
        "yield",
        "yield",
        "yield",
        "yield",
        "yield",
        "yield",
        {
          "relationships": {
            "child": [
              {
                "relationships": {},
                "row": {
                  "id": "1",
                },
              },
              {
                "relationships": {},
                "row": {
                  "id": "2",
                },
              },
            ],
          },
          "row": {
            "id": "1",
          },
        },
        {
          "relationships": {
            "child": [
              {
                "relationships": {},
                "row": {
                  "id": "1",
                },
              },
              {
                "relationships": {},
                "row": {
                  "id": "2",
                },
              },
            ],
          },
          "row": {
            "id": "2",
          },
        },
      ]
    `);
  });

  test('Exists propagates yields from relationship iterator', () => {
    const parent = new YieldSource({tableName: 'parent'});
    const child = new YieldSource({tableName: 'child'});
    const join = new Join({
      parent,
      child,
      parentKey: ['id'],
      childKey: ['id'],
      relationshipName: 'child',
      hidden: false,
      system: 'client',
    });

    const start = new FilterStart(join);
    const exists = new Exists(start, 'child', ['id'], 'EXISTS');
    const end = new FilterEnd(start, exists);
    const catchOp = new Catch(end);

    expect(catchOp.fetch({})).toMatchInlineSnapshot(`
      [
        "yield",
        "yield",
        "yield",
        {
          "relationships": {
            "child": [
              "yield",
              {
                "relationships": {},
                "row": {
                  "id": "1",
                },
              },
              "yield",
              {
                "relationships": {},
                "row": {
                  "id": "2",
                },
              },
            ],
          },
          "row": {
            "id": "1",
          },
        },
        "yield",
        "yield",
        "yield",
        {
          "relationships": {
            "child": [
              "yield",
              {
                "relationships": {},
                "row": {
                  "id": "1",
                },
              },
              "yield",
              {
                "relationships": {},
                "row": {
                  "id": "2",
                },
              },
            ],
          },
          "row": {
            "id": "2",
          },
        },
      ]
    `);
  });

  test('Error propagation during fetch', () => {
    const source = new YieldSource();
    const error = new Error('Fetch failed');
    source.fetch = function* (_req: FetchRequest) {
      yield 'yield';
      throw error;
    };

    const catchOp = new Catch(source);

    expect(() => catchOp.fetch({})).toThrow(error);
  });
});
