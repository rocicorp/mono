import type {Row} from './data.js';

export type ValueType = 'string' | 'number' | 'boolean' | 'null';

/**
 * `related` calls need to know what the available relationships are.
 * The `schema` type encodes this information.
 */
export type SchemaValue = {
  type: ValueType;
  optional?: boolean;
};

export type PrimaryKeys = readonly [string, ...string[]];

/**
 * Information about the nodes output by an operator.
 */
export type Schema = {
  tableName: string;
  primaryKey: PrimaryKeys;
  columns: Record<string, SchemaValue>;
  isHidden: boolean;

  /**
   * Compares two rows in the output of an operator.
   */
  compareRows: (r1: Row, r2: Row) => number;

  relationships?: Record<string, Schema>;
};
