import {Row} from './data.js';

export type ValueType = 'string' | 'number' | 'boolean' | 'null';

// Information about the nodes output by an operator.
export type Schema = {
  tableName: string;
  primaryKey: readonly string[];
  columns: Record<string, ValueType>;

  // relationships: Record<string, Schema>;
  // Compares two rows in the output of an operator.
  compareRows: (r1: Row, r2: Row) => number;

  relationships: Record<string, Schema>;
};
