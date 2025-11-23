
import {describe, expect, test} from 'vitest';
import type {FetchRequest, Input, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import {compareValues, type Node} from './data.ts';
import {FilterEnd, FilterStart} from './filter-operators.ts';
import {Skip} from './skip.ts';
import {Take} from './take.ts';
import {Snitch} from './snitch.ts';

const SCHEMA: SourceSchema = {
  tableName: 'test',
  primaryKey: ['id'],
  columns: {id: {type: 'string'}},
  relationships: {},
  system: 'client',
  sort: [['id', 'asc']],
  compareRows: (a, b) => compareValues(a.id, b.id),
  isHidden: false,
};

class YieldSource implements Input {
  #output: Output | undefined;
  
  setOutput(output: Output): void {
    this.#output = output;
  }
  
  getSchema(): SourceSchema {
    return SCHEMA;
  }
  
  *fetch(req: FetchRequest): Stream<Node | 'yield'> {
    yield 'yield';
    yield {row: {id: '1'}, relationships: {}};
    yield 'yield';
    yield {row: {id: '2'}, relationships: {}};
  }
  
  *cleanup(req: FetchRequest): Stream<Node> {
    // cleanup doesn't yield 'yield' anymore
  }
  
  destroy(): void {}
}

class MockStorage {
  get(key: string) { return undefined; }
  set(key: string, value: any) {}
  del(key: string) {}
}

import {Catch} from './catch.ts';
import {UnionFanIn} from './union-fan-in.ts';
import {Join} from './join.ts';
import {FlippedJoin} from './flipped-join.ts';
import {throwOutput} from './operator.ts';

class RelationalYieldSource extends YieldSource {
  constructor(private readonly name: string) {
    super();
  }

  getSchema(): SourceSchema {
    return {
      ...super.getSchema(),
      tableName: this.name,
    };
  }
}

describe('Yield Propagation', () => {
  test('FilterStart/End propagates yield', () => {
    const source = new YieldSource();
    const start = new FilterStart(source);
    const end = new FilterEnd(start, start);
    const results = [...end.fetch({})];
    expect(results).toEqual(['yield', {row: {id: '1'}, relationships: {}}, 'yield', {row: {id: '2'}, relationships: {}}]);
  });

  test('Skip propagates yield', () => {
    const source = new YieldSource();
    const skip = new Skip(source, {row: {id: ''}, exclusive: false});
    const results = [...skip.fetch({})];
    expect(results).toEqual(['yield', {row: {id: '1'}, relationships: {}}, 'yield', {row: {id: '2'}, relationships: {}}]);
  });

  test('Take propagates yield', () => {
    const source = new YieldSource();
    const take = new Take(source, new MockStorage() as any, 10);
    const results = [...take.fetch({})];
    expect(results).toEqual(['yield', {row: {id: '1'}, relationships: {}}, 'yield', {row: {id: '2'}, relationships: {}}]);
  });

  test('Snitch propagates yield', () => {
    const source = new YieldSource();
    const snitch = new Snitch(source, 'snitch');
    const results = [...snitch.fetch({})];
    expect(results).toEqual(['yield', {row: {id: '1'}, relationships: {}}, 'yield', {row: {id: '2'}, relationships: {}}]);
  });

  test('Catch consumes yield', () => {
    const source = new YieldSource();
    const catchOp = new Catch(source);
    const results = catchOp.fetch({});
    expect(results).toEqual([{row: {id: '1'}, relationships: {}}, {row: {id: '2'}, relationships: {}}]);
  });

  test('UnionFanIn propagates yield', () => {
    const source1 = new YieldSource();
    const source2 = new YieldSource();
    const mockFanOut = {
      getSchema: () => source1.getSchema(),
      setFanIn: () => {},
    } as any;
    const ufi = new UnionFanIn(mockFanOut, [source1, source2]);
    ufi.setOutput(throwOutput);
    const results = [...ufi.fetch({})];
    // mergeFetches dedupes identical rows.
    // YieldSource yields: 'yield', {id: '1'}, 'yield', {id: '2'}
    // Merged: 'yield', 'yield', {id: '1'}, 'yield', 'yield', {id: '2'}
    // Exact order depends on merge implementation but yields should be present.
    expect(results).toContain('yield');
    expect(results.filter(r => r === 'yield').length).toBeGreaterThanOrEqual(4);
    expect(results.filter(r => r !== 'yield').length).toBe(2);
  });

  test('Join propagates parent yield', () => {
    const parent = new RelationalYieldSource('parent');
    const child = new RelationalYieldSource('child');
    const join = new Join({
      parent,
      child,
      storage: new MockStorage() as any,
      parentKey: ['id'],
      childKey: ['id'],
      relationshipName: 'child',
      hidden: false,
      system: 'client',
    });
    join.setOutput(throwOutput);
    const results = [...join.fetch({})];
    // Parent yields should be propagated.
    // Child yields are inside the relationship stream (not visible here unless we iterate relationship).
    expect(results).toContain('yield');
    const rows = results.filter(r => r !== 'yield') as Node[];
    expect(rows.length).toBe(2);
    expect(rows[0].row).toEqual({id: '1'});
    expect(rows[1].row).toEqual({id: '2'});
  });

  test('FlippedJoin propagates child yield', () => {
    const parent = new RelationalYieldSource('parent');
    const child = new RelationalYieldSource('child');
    const join = new FlippedJoin({
      parent,
      child,
      parentKey: ['id'],
      childKey: ['id'],
      relationshipName: 'child',
      hidden: false,
      system: 'client',
    });
    join.setOutput(throwOutput);
    const results = [...join.fetch({})];
    // Child yields should be propagated.
    expect(results).toContain('yield');
    // FlippedJoin outputs PARENT nodes.
    // Since parent also has id 1 and 2, and child has id 1 and 2.
    // It should match.
    const rows = results.filter(r => r !== 'yield') as Node[];
    expect(rows.length).toBe(2);
    expect(rows[0].row).toEqual({id: '1'});
    expect(rows[1].row).toEqual({id: '2'});
  });
});
