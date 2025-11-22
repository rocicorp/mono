
import {describe, expect, test} from 'vitest';
import type {FetchRequest, Input, Output} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import type {Stream} from './stream.ts';
import {compareValues, type Node} from './data.ts';
import {FilterEnd, FilterStart} from './filter-operators.ts';
import {Skip} from './skip.ts';
import {Take} from './take.ts';
import {Snitch} from './snitch.ts';

class YieldSource implements Input {
  #output: Output | undefined;
  
  setOutput(output: Output): void {
    this.#output = output;
  }
  
  getSchema(): SourceSchema {
    return {
      tableName: 'test',
      primaryKey: ['id'],
      columns: {id: {type: 'string'}},
      relationships: {},
      system: 'client',
      sort: [['id', 'asc']],
      compareRows: (a, b) => compareValues(a.id, b.id),
      isHidden: false,
    };
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
});
