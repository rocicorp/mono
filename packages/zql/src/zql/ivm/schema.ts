import type {Ordering} from '../ast/ast.js';
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

// TODO: It would be cleaner to not have zero-protocol depend on zql. This means
// that we would need to move the ast types out of zql and into zero-protocol
// (or into a different package that both depend on).
export type PrimaryKey = readonly [string] | readonly [string, ...string[]];

export type TableSchemaBase = {
  readonly tableName: string;
  readonly primaryKey: PrimaryKey;
  readonly columns: Record<string, SchemaValue>;
};
/**
 * Information about the nodes output by an operator.
 */
export type TableSchema = TableSchemaBase & {
  readonly relationships: {[key: string]: TableSchema};
  readonly isHidden: boolean;
  readonly compareRows: (r1: Row, r2: Row) => number;
  readonly sort: Ordering;
};

// TODO(arv): Make all schemas use same hidden class
