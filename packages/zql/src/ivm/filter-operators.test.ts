import {describe, expect, test, vi} from 'vitest';
import {FilterStart, type FilterOutput} from './filter-operators.ts';
import type {FetchRequest, Input} from './operator.ts';
import type {SourceSchema} from './schema.ts';
import {drainGenerator, pullOf} from './stream.ts';

describe('FilterStart', () => {
  test('fetch calls endFilter even if stream is not fully consumed', () => {
    const mockInput: Input = {
      setOutput: vi.fn(),
      fetch: (_req: FetchRequest) =>
        pullOf([
          {row: {id: 1}, relationships: {}},
          {row: {id: 2}, relationships: {}},
          {row: {id: 3}, relationships: {}},
        ]),
      destroy: vi.fn(),
      getSchema: vi.fn(() => ({}) as SourceSchema),
    };

    const mockFilterOutput: FilterOutput = {
      push: vi.fn(),
      beginFilter: vi.fn(),
      filter: () => drainGenerator(filterGenerator()),
      endFilter: vi.fn(),
    };

    const filterStart = new FilterStart(mockInput);
    filterStart.setFilterOutput(mockFilterOutput);

    const stream = filterStart.fetch({} as FetchRequest);
    try {
      for (let n = stream.next(); n !== undefined; n = stream.next()) {
        expect(n).toEqual({row: {id: 1}, relationships: {}});
        // break after consuming 1 of the 3 nodes.
        break;
      }
    } finally {
      stream.close();
    }

    expect(mockFilterOutput.beginFilter).toHaveBeenCalledTimes(1);
    expect(mockFilterOutput.endFilter).toHaveBeenCalledTimes(1);
  });
});

function* filterGenerator(): Generator<'yield', boolean> {
  return true;
}
