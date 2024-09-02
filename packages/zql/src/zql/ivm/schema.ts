import type {Row} from './data.js';

export type ValueType = 'string' | 'number' | 'boolean' | 'null';

export type PrimaryKeys = readonly [string, ...string[]];

/**
 * Information about the nodes output by an operator.
 */
export type Schema = {
  tableName: string;
  primaryKey: PrimaryKeys;
  columns: Record<string, ValueType>;
  isHidden: boolean;

  /**
   * Compares two rows in the output of an operator.
   */
  compareRows: (r1: Row, r2: Row) => number;

  relationships: Record<string, Schema>;
};
