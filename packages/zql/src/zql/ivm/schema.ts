import { Ordering } from '../ast/ast.js';
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

export type SchemaBase = {
  readonly tableName: string;
  readonly primaryKey: PrimaryKeys;
  readonly columns: Record<string, SchemaValue>;
};
/**
 * Information about the nodes output by an operator.
 */
export type Schema = SchemaBase & {
  readonly relationships?: {[key: string]: Schema};
  readonly isHidden: boolean;
  readonly compareRows: (r1: Row, r2: Row) => number;
  sort: Ordering;
};
