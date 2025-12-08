import {describe, expect, test, vi} from 'vitest';
import {Exists} from './exists.ts';
import type {FilterInput, FilterOutput} from './filter-operators.ts';

describe('Exists', () => {
  test('forwards beginFilter/endFilter', () => {
    const mockInput = {
      setFilterOutput: vi.fn(),
      getSchema: vi.fn(() => ({
        relationships: {
          rel: {
            type: 'many',
            source: 'child',
            sourceField: ['childID'],
            destField: ['id'],
          },
        },
        primaryKey: ['id'],
      })),
      destroy: vi.fn(),
    } as unknown as FilterInput;

    const exists = new Exists(mockInput, 'rel', ['id'], 'EXISTS');

    const mockOutput = {
      push: vi.fn(),
      filter: vi.fn(),
      beginFilter: vi.fn(),
      endFilter: vi.fn(),
    } as unknown as FilterOutput;

    exists.setFilterOutput(mockOutput);

    exists.beginFilter();
    expect(mockOutput.beginFilter).toHaveBeenCalled();

    exists.endFilter();
    expect(mockOutput.endFilter).toHaveBeenCalled();
  });
});
