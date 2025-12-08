/* oxlint-disable @typescript-eslint/no-explicit-any */
import {describe, expect, test, vi} from 'vitest';
import type {Node} from './data.js';
import {skipYields, type FetchRequest, type Operator} from './operator.js';
import type {SourceSchema} from './schema.js';
import {UnionFanIn} from './union-fan-in.js';
import type {UnionFanOut} from './union-fan-out.js';

const mockSchema: SourceSchema = {
  tableName: 'test',
  columns: {},
  primaryKey: ['id'],
  relationships: {},
  compareRows: (a, b) => (a.id as number) - (b.id as number),
  isHidden: false,
  sort: [],
  system: 'client',
};

const mockOperator = (schema: SourceSchema, data: Node[] = []): Operator => ({
  getSchema: () => schema,
  fetch: (_req: FetchRequest) => data,
  push: vi.fn(),
  setOutput: vi.fn(),
  destroy: vi.fn(),
});

const mockUnionFanOut = (schema: SourceSchema): UnionFanOut =>
  ({
    getSchema: () => schema,
    setFanIn: vi.fn(),
    fetch: vi.fn(),
    push: vi.fn(),
    setOutput: vi.fn(),
    destroy: vi.fn(),
  }) as any;

describe('UnionFanIn', () => {
  describe('schema creation', () => {
    test('creates schema from fanOut and merges relationships from inputs', () => {
      const fanOutSchema: SourceSchema = {
        ...mockSchema,
        relationships: {
          fanOutRel: {} as any,
        },
      };

      const input1Schema: SourceSchema = {
        ...mockSchema,
        relationships: {
          input1Rel: {} as any,
        },
      };

      const input2Schema: SourceSchema = {
        ...mockSchema,
        relationships: {
          input2Rel: {} as any,
        },
      };

      const fanOut = mockUnionFanOut(fanOutSchema);
      const input1 = mockOperator(input1Schema);
      const input2 = mockOperator(input2Schema);

      const fanIn = new UnionFanIn(fanOut, [input1, input2]);
      const resultSchema = fanIn.getSchema();

      expect(resultSchema.tableName).toBe('test');
      expect(resultSchema.primaryKey).toEqual(['id']);
      expect(Object.keys(resultSchema.relationships)).toEqual(
        expect.arrayContaining(['fanOutRel', 'input1Rel', 'input2Rel']),
      );
    });

    test('preserves all schema properties from fanOut', () => {
      const compareRows = (a: any, b: any) => a.id - b.id;
      const fanOutSchema: SourceSchema = {
        tableName: 'custom',
        columns: {col1: {type: 'string'}},
        primaryKey: ['id', 'name'],
        relationships: {},
        compareRows,
        isHidden: true,
        sort: [['name', 'asc']],
        system: 'client',
      };

      const fanOut = mockUnionFanOut(fanOutSchema);
      const fanIn = new UnionFanIn(fanOut, []);

      const resultSchema = fanIn.getSchema();

      expect(resultSchema.tableName).toBe('custom');
      expect(resultSchema.columns).toEqual({col1: {type: 'string'}});
      expect(resultSchema.primaryKey).toEqual(['id', 'name']);
      expect(resultSchema.compareRows).toBe(compareRows);
      expect(resultSchema.isHidden).toBe(true);
      expect(resultSchema.sort).toEqual([['name', 'asc']]);
      expect(resultSchema.system).toBe('client');
    });

    test('throws when input schemas have mismatched table names', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator({
        ...mockSchema,
        tableName: 'different',
      });

      expect(() => new UnionFanIn(fanOut, [input1])).toThrow(
        'Table name mismatch in union fan-in',
      );
    });

    test('throws when input schemas have mismatched primary keys', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator({
        ...mockSchema,
        primaryKey: ['id', 'name'],
      });

      expect(() => new UnionFanIn(fanOut, [input1])).toThrow(
        'Primary key mismatch in union fan-in',
      );
    });

    test('throws when input schemas have mismatched system', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator({
        ...mockSchema,
        system: 'test',
      });

      expect(() => new UnionFanIn(fanOut, [input1])).toThrow(
        'System mismatch in union fan-in',
      );
    });

    test('throws when input schemas have mismatched compareRows', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator({
        ...mockSchema,
        compareRows: () => 0,
      });

      expect(() => new UnionFanIn(fanOut, [input1])).toThrow(
        'compareRows mismatch in union fan-in',
      );
    });

    test('throws when input schemas have mismatched sort', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator({
        ...mockSchema,
        sort: [['name', 'asc']],
      });

      expect(() => new UnionFanIn(fanOut, [input1])).toThrow(
        'Sort mismatch in union fan-in',
      );
    });

    test('throws when relationship names conflict between inputs', () => {
      const input1Schema: SourceSchema = {
        ...mockSchema,
        relationships: {
          sharedRel: {} as any,
        },
      };

      const input2Schema: SourceSchema = {
        ...mockSchema,
        relationships: {
          sharedRel: {} as any,
        },
      };

      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator(input1Schema);
      const input2 = mockOperator(input2Schema);

      expect(() => new UnionFanIn(fanOut, [input1, input2])).toThrow(
        'Relationship sharedRel exists in multiple upstream inputs to union fan-in',
      );
    });

    test('handles empty inputs array', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const fanIn = new UnionFanIn(fanOut, []);

      const resultSchema = fanIn.getSchema();
      expect(resultSchema).toEqual(mockSchema);
    });
  });

  describe('fetch', () => {
    test('merges results from multiple inputs in sorted order', () => {
      const data1: Node[] = [
        {row: {id: 1}, relationships: {}},
        {row: {id: 3}, relationships: {}},
      ];
      const data2: Node[] = [
        {row: {id: 2}, relationships: {}},
        {row: {id: 4}, relationships: {}},
      ];

      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator(mockSchema, data1);
      const input2 = mockOperator(mockSchema, data2);

      const fanIn = new UnionFanIn(fanOut, [input1, input2]);
      const result = Array.from(skipYields(fanIn.fetch({} as FetchRequest)));

      expect(result).toHaveLength(4);
      expect(result.map(n => n.row.id)).toEqual([1, 2, 3, 4]);
    });

    test('handles empty inputs', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const fanIn = new UnionFanIn(fanOut, []);

      const result = Array.from(fanIn.fetch({} as FetchRequest));
      expect(result).toHaveLength(0);
    });

    test('deduplicates identical rows', () => {
      const data1: Node[] = [
        {row: {id: 1}, relationships: {}},
        {row: {id: 2}, relationships: {}},
      ];
      const data2: Node[] = [
        {row: {id: 2}, relationships: {}},
        {row: {id: 3}, relationships: {}},
      ];

      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator(mockSchema, data1);
      const input2 = mockOperator(mockSchema, data2);

      const fanIn = new UnionFanIn(fanOut, [input1, input2]);
      const result = Array.from(skipYields(fanIn.fetch({} as FetchRequest)));

      expect(result).toHaveLength(3);
      expect(result.map(n => n.row.id)).toEqual([1, 2, 3]);
    });
  });

  describe('destroy', () => {
    test('destroys all input operators', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const input1 = mockOperator(mockSchema);
      const input2 = mockOperator(mockSchema);

      const fanIn = new UnionFanIn(fanOut, [input1, input2]);
      fanIn.destroy();

      expect(input1.destroy).toHaveBeenCalled();
      expect(input2.destroy).toHaveBeenCalled();
    });

    test('handles empty inputs', () => {
      const fanOut = mockUnionFanOut(mockSchema);
      const fanIn = new UnionFanIn(fanOut, []);

      expect(() => fanIn.destroy()).not.toThrow();
    });
  });
});
