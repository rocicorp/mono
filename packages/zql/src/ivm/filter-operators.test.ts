import {describe, expect, test, vi} from 'vitest';
import type {NoSubqueryCondition} from '../builder/filter.ts';
import {FilterStart, type FilterOutput} from './filter-operators.ts';
import type {FetchRequest, Input} from './operator.ts';
import type {SourceSchema} from './schema.ts';

describe('FilterStart', () => {
  test('fetch calls endFilter even if stream is not fully consumed', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: function* (_req: FetchRequest) {
        yield {row: {id: 1}, relationships: {}};
        yield {row: {id: 2}, relationships: {}};
        yield {row: {id: 3}, relationships: {}};
      },
      destroy: vi.fn(),
      getSchema: vi.fn(() => ({}) as SourceSchema),
    };

    const mockFilterOutput: FilterOutput = {
      push: vi.fn(),
      beginFilter: vi.fn(),
      filter: filterGenerator,
      endFilter: vi.fn(),
    };

    const filterStart = new FilterStart(mockInput);
    filterStart.setFilterOutput(mockFilterOutput);

    for (const n of filterStart.fetch({} as FetchRequest)) {
      expect(n).toEqual({row: {id: 1}, relationships: {}});
      // break after consuming 1 of the 3 nodes.
      break;
    }

    expect(mockFilterOutput.beginFilter).toHaveBeenCalledTimes(1);
    expect(mockFilterOutput.endFilter).toHaveBeenCalledTimes(1);
  });

  test('passes the same merged filter for the same received filter', () => {
    const received: (FetchRequest['filter'] | undefined)[] = [];
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: function* (req: FetchRequest) {
        received.push(req.filter);
      },
      destroy: vi.fn(),
      getSchema: vi.fn(() => ({}) as SourceSchema),
    };
    const own: NoSubqueryCondition = {
      type: 'simple',
      left: {type: 'column', name: 'a'},
      op: '=',
      right: {type: 'literal', value: 1},
    };
    const outer: NoSubqueryCondition = {
      type: 'simple',
      left: {type: 'column', name: 'b'},
      op: '=',
      right: {type: 'literal', value: 2},
    };

    const filterStart = new FilterStart(mockInput, own);
    filterStart.setFilterOutput({
      push: vi.fn(),
      beginFilter: vi.fn(),
      filter: filterGenerator,
      endFilter: vi.fn(),
    });
    for (const req of [{}, {filter: outer}, {filter: outer}, {}]) {
      [...filterStart.fetch(req)];
    }

    expect(received[0]).toBe(own);
    expect(received[1]).toEqual({type: 'and', conditions: [outer, own]});
    expect(received[2]).toBe(received[1]);
    expect(received[3]).toBe(own);
  });
});

function* filterGenerator(): Generator<'yield', boolean> {
  return true;
}
