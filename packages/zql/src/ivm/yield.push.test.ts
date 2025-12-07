import {describe, expect, test} from 'vitest';
import type {InputBase, FetchRequest} from './operator.ts';
import type {Stream} from './stream.ts';
import type {Node} from './data.ts';
import {FilterEnd, FilterStart, type FilterOutput} from './filter-operators.ts';
import {Skip} from './skip.ts';
import {Take} from './take.ts';
import {Join} from './join.ts';
import {UnionFanIn} from './union-fan-in.ts';
import {UnionFanOut} from './union-fan-out.ts';
import {FlippedJoin} from './flipped-join.ts';
import {Exists} from './exists.ts';
import {FanOut} from './fan-out.ts';
import {FanIn} from './fan-in.ts';
import {Filter} from './filter.ts';
import type {Change} from './change.ts';
import {MemorySource} from './memory-source.ts';
import {MemoryStorage} from './memory-storage.ts';
import type {SourceChange, SourceInput} from './source.ts';
import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {consume} from './stream.ts';
import type {PrimaryKey} from '../../../zero-types/src/schema.ts';

class YieldOutput implements FilterOutput {
  yields: boolean = false;

  *push(_change: Change | SourceChange, _pusher: InputBase): Stream<'yield'> {
    if (this.yields) yield 'yield';
    // Consume change
    if (this.yields) yield 'yield';
  }

  beginFilter() {}
  endFilter() {}
  *filter(_node: Node): Generator<'yield', boolean> {
    if (this.yields) yield 'yield';
    return true;
  }
}

class YieldMemorySource extends MemorySource {
  yieldOnFetch = false;

  constructor(schema?: {
    tableName: string;
    primaryKey: PrimaryKey;
    columns: Record<string, SchemaValue>;
  }) {
    const defaultSchema = {
      tableName: 'table1',
      primaryKey: ['id'] as const,
      columns: {id: {type: 'string'}} as Record<string, SchemaValue>,
    };
    const s = schema ?? defaultSchema;
    super(s.tableName, s.columns, s.primaryKey);
  }

  override connect(
    sort: Ordering,
    filters?: Condition,
    splitEditKeys?: Set<string>,
  ): SourceInput {
    const input = super.connect(sort, filters, splitEditKeys);
    const originalFetch = input.fetch.bind(input);

    const source = this;
    input.fetch = function* (req: FetchRequest): Stream<Node | 'yield'> {
      for (const n of originalFetch(req)) {
        if (source.yieldOnFetch) {
          yield 'yield';
        }
        yield n;
      }
      if (source.yieldOnFetch) yield 'yield';
    };
    return input;
  }
}

const CHILD_SCHEMA = {
  tableName: 'child',
  primaryKey: ['id'] as const,
  columns: {
    id: {type: 'string'},
    parentId: {type: 'string'},
  } as Record<string, SchemaValue>,
} as const;

function collectPush(
  source: YieldMemorySource,
  change: SourceChange,
): ('yield' | undefined)[] {
  const result: ('yield' | undefined)[] = [];
  for (const r of source.push(change)) {
    if (r === 'yield') {
      result.push(r);
    }
  }
  return result;
}

function makeAdd(id: string): SourceChange {
  return {type: 'add', row: {id}};
}

function makeRemove(id: string): SourceChange {
  return {type: 'remove', row: {id}};
}

function makeEdit(id: string, oldId: string): SourceChange {
  return {
    type: 'edit',
    row: {id},
    oldRow: {id: oldId},
  };
}

describe('Yield Propagation (Push)', () => {
  describe('FilterStart/End', () => {
    test('propagates yield from output push', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = false;

      const start = new FilterStart(source.connect([['id', 'asc']]));
      const end = new FilterEnd(start, start);
      const output = new YieldOutput(); // Yields
      output.yields = true;

      end.setOutput(output);
      start.setFilterOutput(end);

      // YieldOutput yields 2.
      expect(collectPush(source, makeAdd('1'))).toEqual(['yield', 'yield']);
    });
  });

  describe('Skip', () => {
    test('propagates yield from output push', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = false;
      const skip = new Skip(source.connect([['id', 'asc']]), {
        row: {id: ''},
        exclusive: false,
      });
      const output = new YieldOutput();
      output.yields = true;
      skip.setOutput(output);

      expect(collectPush(source, makeAdd('1'))).toEqual(['yield', 'yield']);
    });
  });

  describe('Take', () => {
    test('propagates yield from output push add', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = false;

      const storage = new MemoryStorage();
      const take = new Take(source.connect([['id', 'asc']]), storage, 1);
      const output = new YieldOutput();
      output.yields = true;
      take.setOutput(output);

      // Initialize Take
      consume(take.fetch({}));

      expect(collectPush(source, makeAdd('0'))).toEqual(['yield', 'yield']);
    });

    test('propagates yield from output push remove', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = false;

      consume(source.push(makeAdd('0')));

      const storage = new MemoryStorage();
      const take = new Take(source.connect([['id', 'asc']]), storage, 3);
      const output = new YieldOutput();
      output.yields = true;
      take.setOutput(output);

      // Initialize Take
      consume(take.fetch({}));

      expect(collectPush(source, makeRemove('0'))).toEqual(['yield', 'yield']);
    });

    test('propagates yield from fetch during push (add, displacement)', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = true;

      // Initialize with data
      consume(source.push(makeAdd('1')));
      consume(source.push(makeAdd('2')));

      const storage = new MemoryStorage();
      const take = new Take(source.connect([['id', 'asc']]), storage, 1);
      const output = new YieldOutput();
      output.yields = false;
      take.setOutput(output);

      // Initialize Take
      consume(take.fetch({}));

      // Push add '0'. This should displace '1'.
      const result = collectPush(source, makeAdd('0'));
      // Take fetches to find bound to remove. It finds 1 node and stops.
      // YieldMemorySource yields before each node (1) + at end (but not reached due to early break).
      expect(result).toEqual(['yield']);
    });

    test('propagates yield from fetch during push (remove)', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = true;

      // Initialize with data
      consume(source.push(makeAdd('0')));
      consume(source.push(makeAdd('1')));

      const storage = new MemoryStorage();
      const take = new Take(source.connect([['id', 'asc']]), storage, 1);
      const output = new YieldOutput();
      output.yields = false;

      take.setOutput(output);

      // Initialize Take
      consume(take.fetch({}));

      // Push remove '0'.
      const result = collectPush(source, makeRemove('0'));
      // Take fetches to find replacement.
      expect(result).toEqual(['yield', 'yield']);
    });

    test('propagates yield from fetch during push (edit, move out)', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = true;

      // Initialize with data
      consume(source.push(makeAdd('0')));
      consume(source.push(makeAdd('1')));

      const storage = new MemoryStorage();
      const take = new Take(source.connect([['id', 'asc']]), storage, 1);
      const output = new YieldOutput();
      output.yields = false;
      take.setOutput(output);

      // Initialize Take
      consume(take.fetch({}));

      // Edit '0' to '2' (move out of bounds).
      const result = collectPush(source, makeEdit('2', '0'));
      // Take fetches to find replacement. It finds 1 node and stops.
      // YieldMemorySource yields before each node (1).
      expect(result).toEqual(['yield']);
    });

    test('propagates yield from fetch during push (edit, move in)', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = true;

      // Initialize with data
      consume(source.push(makeAdd('1')));
      consume(source.push(makeAdd('2')));

      const storage = new MemoryStorage();
      const take = new Take(source.connect([['id', 'asc']]), storage, 1);
      const output = new YieldOutput();
      output.yields = false;
      take.setOutput(output);

      // Initialize Take
      consume(take.fetch({}));

      // Edit '2' to '0' (move into bounds).
      const result = collectPush(source, makeEdit('0', '2'));
      // Take fetches to find old bound to remove.
      // Test shows 2 yields.
      expect(result).toEqual(['yield', 'yield']);
    });
  });

  describe('Join', () => {
    test("doesn't propagate yield from child fetch when pushing to parent, because doesn't consume child fetch", () => {
      const parent = new YieldMemorySource();
      parent.yieldOnFetch = false;
      const child = new YieldMemorySource(CHILD_SCHEMA);
      child.yieldOnFetch = true;

      // Initialize data
      consume(parent.push(makeAdd('1')));
      consume(child.push({type: 'add', row: {id: 'c1', parentId: '1'}}));
      consume(child.push({type: 'add', row: {id: 'c2', parentId: '2'}}));

      const join = new Join({
        parent: parent.connect([['id', 'asc']]),
        child: child.connect([['id', 'asc']]),
        parentKey: ['id'],
        childKey: ['parentId'],
        relationshipName: 'child',
        hidden: false,
        system: 'client',
      });
      const output = new YieldOutput();
      output.yields = false;
      join.setOutput(output);

      // Join push does not iterate the child fetch.
      // It attaches the fetch iterator to the node relationship map.
      // Since the iterator is not consumed, YieldMemorySource.fetch generator is not executed.
      // Total 0 yields.
      expect(collectPush(parent, makeAdd('2'))).toEqual([]);
    });

    test('propagates yield from parent fetch when pushing to child', () => {
      const parent = new YieldMemorySource();
      parent.yieldOnFetch = true;
      const child = new YieldMemorySource(CHILD_SCHEMA);
      child.yieldOnFetch = false;

      // Initialize parent with matching data
      consume(parent.push(makeAdd('1')));

      const join = new Join({
        parent: parent.connect([['id', 'asc']]),
        child: child.connect([['id', 'asc']]),
        parentKey: ['id'],
        childKey: ['parentId'],
        relationshipName: 'child',
        hidden: false,
        system: 'client',
      });
      const output = new YieldOutput();
      output.yields = false;
      join.setOutput(output);

      // Push to child with parentId='1' which matches the existing parent.
      // Join fetches from parent. Parent has 1 matching node, so fetch yields 2 (1 before node + 1 at end).
      expect(
        collectPush(child, {
          type: 'add',
          row: {id: 'c1', parentId: '1'},
        }),
      ).toEqual(['yield', 'yield']);
    });

    test('propagates yield from output push', () => {
      const parent = new YieldMemorySource();
      parent.yieldOnFetch = false;
      const child = new YieldMemorySource(CHILD_SCHEMA);
      child.yieldOnFetch = false;

      // Initialize parent with matching data
      consume(parent.push(makeAdd('1')));

      const join = new Join({
        parent: parent.connect([['id', 'asc']]),
        child: child.connect([['id', 'asc']]),
        parentKey: ['id'],
        childKey: ['parentId'],
        relationshipName: 'child',
        hidden: false,
        system: 'client',
      });
      const output = new YieldOutput();
      output.yields = true;
      join.setOutput(output);

      // Push to child with matching parent. YieldOutput yields.
      expect(
        collectPush(child, {
          type: 'add',
          row: {id: 'c1', parentId: '1'},
        }),
      ).toEqual(['yield', 'yield']);
    });
  });

  describe('FlippedJoin', () => {
    test('propagates yield from fetch when pushing to child', () => {
      const parent = new YieldMemorySource();
      parent.yieldOnFetch = true;
      const child = new YieldMemorySource(CHILD_SCHEMA);
      child.yieldOnFetch = false;

      // Initialize data
      consume(parent.push(makeAdd('1')));
      consume(child.push({type: 'add', row: {id: 'c1', parentId: '1'}}));

      const flippedJoin = new FlippedJoin({
        parent: parent.connect([['id', 'asc']]),
        child: child.connect([['id', 'asc']]),
        parentKey: ['id'],
        childKey: ['parentId'],
        relationshipName: 'child',
        hidden: false,
        system: 'client',
      });
      const output = new YieldOutput();
      output.yields = false;
      flippedJoin.setOutput(output);

      // When pushing to child, FlippedJoin fetches the parent to join with the new child row.
      // Push child with parentId='1' which matches the existing parent.
      // Parent fetch yields 2 (1 before node + 1 at end).
      expect(
        collectPush(child, {type: 'add', row: {id: 'c2', parentId: '1'}}),
      ).toEqual(['yield', 'yield']);
    });

    test('propagates yield from child fetch when pushing to parent', () => {
      const parent = new YieldMemorySource();
      parent.yieldOnFetch = false;
      const child = new YieldMemorySource(CHILD_SCHEMA);
      child.yieldOnFetch = true;

      consume(child.push({type: 'add', row: {id: 'c1', parentId: '1'}}));

      const flippedJoin = new FlippedJoin({
        parent: parent.connect([['id', 'asc']]),
        child: child.connect([['id', 'asc']]),
        parentKey: ['id'],
        childKey: ['parentId'],
        relationshipName: 'child',
        hidden: false,
        system: 'client',
      });
      const output = new YieldOutput();
      output.yields = false;
      flippedJoin.setOutput(output);

      // When pushing to parent, FlippedJoin fetches children to check if any exist (inner join).
      // No children exist, so fetch yields 1 (only end yield).
      expect(collectPush(parent, makeAdd('1'))).toEqual(['yield']);
    });

    test('propagates yield from output push', () => {
      const parent = new YieldMemorySource();
      parent.yieldOnFetch = false;
      const child = new YieldMemorySource(CHILD_SCHEMA);
      child.yieldOnFetch = false;

      // Initialize child with matching data
      consume(child.push({type: 'add', row: {id: 'c1', parentId: '1'}}));

      const flippedJoin = new FlippedJoin({
        parent: parent.connect([['id', 'asc']]),
        child: child.connect([['id', 'asc']]),
        parentKey: ['id'],
        childKey: ['parentId'],
        relationshipName: 'child',
        hidden: false,
        system: 'client',
      });
      const output = new YieldOutput();
      output.yields = true;
      flippedJoin.setOutput(output);

      // Push to parent with matching child. YieldOutput yields.
      expect(collectPush(parent, makeAdd('1'))).toEqual(['yield', 'yield']);
    });
  });

  describe('Exists', () => {
    test('propagates yield from output push', () => {
      const parent = new YieldMemorySource();
      parent.yieldOnFetch = false;
      const child = new YieldMemorySource(CHILD_SCHEMA);
      child.yieldOnFetch = false;

      consume(child.push({type: 'add', row: {id: 'c1', parentId: '1'}}));

      const join = new Join({
        parent: parent.connect([['id', 'asc']]),
        child: child.connect([['id', 'asc']]),
        parentKey: ['id'],
        childKey: ['parentId'],
        relationshipName: 'child',
        hidden: false,
        system: 'client',
      });

      const start = new FilterStart(join);
      const exists = new Exists(start, 'child', ['id'], 'EXISTS');
      const end = new FilterEnd(start, exists);
      const output = new YieldOutput();
      output.yields = true;
      end.setOutput(output);

      // Two from output
      expect(collectPush(parent, makeAdd('1'))).toEqual(['yield', 'yield']);
    });

    test('propagates yield from child exist check', () => {
      const parent = new YieldMemorySource();
      const child = new YieldMemorySource(CHILD_SCHEMA);
      child.yieldOnFetch = true;

      consume(child.push({type: 'add', row: {id: 'c1', parentId: '1'}}));
      consume(child.push({type: 'add', row: {id: 'c2', parentId: '1'}}));

      const join = new Join({
        parent: parent.connect([['id', 'asc']]),
        child: child.connect([['id', 'asc']]),
        parentKey: ['id'],
        childKey: ['parentId'],
        relationshipName: 'child',
        hidden: false,
        system: 'client',
      });

      const start = new FilterStart(join);
      const exists = new Exists(start, 'child', ['id'], 'EXISTS');
      const end = new FilterEnd(start, exists);
      const output = new YieldOutput();
      output.yields = false;
      end.setOutput(output);

      // child fetch yields 3 (2 matching child + 1 at end).
      expect(collectPush(parent, makeAdd('1'))).toEqual([
        'yield',
        'yield',
        'yield',
      ]);
    });
  });

  describe('FanOut', () => {
    test('propagates yield from output push', () => {
      const source = new YieldMemorySource();
      const start = new FilterStart(source.connect([['id', 'asc']]));
      const fanOut = new FanOut(start);

      // Create two branches with Filter operators
      const filter1 = new Filter(fanOut, () => true);
      const filter2 = new Filter(fanOut, () => true);
      fanOut.setFilterOutput(filter1);
      fanOut.setFilterOutput(filter2);

      // FanOut requires FanIn to be set for push to work
      const fanIn = new FanIn(fanOut, [filter1, filter2]);
      fanOut.setFanIn(fanIn);

      const output = new YieldOutput();
      output.yields = true;
      fanIn.setFilterOutput(output);

      // FanOut pushes to 2 branches, FanIn combines and pushes to YieldOutput.
      // Each branch push to FanIn, then FanIn pushes to YieldOutput (which yields).
      expect(collectPush(source, makeAdd('1'))).toEqual(['yield', 'yield']);
    });
  });

  describe('FanIn', () => {
    test('propagates yield from output push', () => {
      const source = new YieldMemorySource();
      const start = new FilterStart(source.connect([['id', 'asc']]));

      // FanIn needs a FanOut to get schema
      const fanOut = new FanOut(start);

      // Create two branches with Filter operators
      const filter1 = new Filter(fanOut, () => true);
      const filter2 = new Filter(fanOut, () => true);
      fanOut.setFilterOutput(filter1);
      fanOut.setFilterOutput(filter2);

      const fanIn = new FanIn(fanOut, [filter1, filter2]);
      fanOut.setFanIn(fanIn);

      const output = new YieldOutput();
      output.yields = true;
      fanIn.setFilterOutput(output);

      // FanOut pushes to 2 branches, FanIn combines and pushes to YieldOutput.
      expect(collectPush(source, makeAdd('1'))).toEqual(['yield', 'yield']);
    });
  });

  describe('Filter', () => {
    test('propagates yield from output push', () => {
      const source = new YieldMemorySource();
      const start = new FilterStart(source.connect([['id', 'asc']]));
      const filter = new Filter(start, () => true);
      const output = new YieldOutput();
      output.yields = true;
      filter.setFilterOutput(output);

      expect(collectPush(source, makeAdd('1'))).toEqual(['yield', 'yield']);
    });
  });

  describe('UnionFanOut', () => {
    test('propagates yield from output push', () => {
      const source = new YieldMemorySource();
      const ufo = new UnionFanOut(source.connect([['id', 'asc']]));
      const output1 = new YieldOutput();
      output1.yields = true;
      const output2 = new YieldOutput();
      output2.yields = true;
      ufo.setOutput(output1);
      ufo.setOutput(output2);

      new UnionFanIn(ufo, []);

      // UnionFanOut yields from both outputs.
      expect(collectPush(source, makeAdd('1'))).toEqual([
        'yield',
        'yield',
        'yield',
        'yield',
      ]);
    });
  });

  describe('Error Propagation', () => {
    test('propagates error from input push', () => {
      const source = new YieldMemorySource();
      const output = new YieldOutput();
      output.yields = false;
      source.connect([['id', 'asc']]).setOutput(output);

      const error = new Error('Push failed');
      // Override push to throw
      source.push = function* (_change: SourceChange) {
        yield 'yield';
        throw error;
      };

      const generator = source.push(makeAdd('1')) as Generator<'yield'>;
      expect(generator.next()).toEqual({value: 'yield', done: false});
      expect(() => generator.next()).toThrow(error);
    });
  });

  describe('Deep Pipeline', () => {
    test('propagates yield through multiple operators', () => {
      const source = new YieldMemorySource();
      source.yieldOnFetch = false;
      const start = new FilterStart(source.connect([['id', 'asc']]));
      const filter = new Filter(start, () => true);
      const end = new FilterEnd(start, filter);
      const storage = new MemoryStorage();
      const take = new Take(end, storage, 10);
      const output = new YieldOutput();
      output.yields = true;
      take.setOutput(output);

      consume(take.fetch({}));

      // Output yields 2.
      expect(collectPush(source, makeAdd('1'))).toEqual(['yield', 'yield']);
    });
  });
});
