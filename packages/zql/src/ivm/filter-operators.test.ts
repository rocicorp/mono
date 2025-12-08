import {describe, expect, test, vi} from 'vitest';
import type {FetchRequest, Input} from './operator.ts';
import {FilterStart, type FilterOutput} from './filter-operators.ts';
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
});

function* filterGenerator(): Generator<'yield', boolean> {
  return true;
}
