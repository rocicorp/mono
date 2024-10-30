import type {Ordering} from '../../../../zero-protocol/src/ast.js';
import type {Row} from '../../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../../zero-protocol/src/primary-key.js';

export type ValueType = 'string' | 'number' | 'boolean' | 'null' | 'json';

/**
 * `related` calls need to know what the available relationships are.
 * The `schema` type encodes this information.
 */
export type SchemaValue = {
  type: ValueType;
  optional?: boolean;
};

export type SourceOrTableSchema = {
  readonly tableName: string;
  readonly primaryKey: PrimaryKey;
  readonly columns: Record<string, SchemaValue>;
};
/**
 * Information about the nodes output by an operator.
 */
export type TableSchema = SourceOrTableSchema & {
  readonly relationships: {[key: string]: TableSchema};
  readonly isHidden: boolean;
  readonly compareRows: (r1: Row, r2: Row) => number;
  readonly sort: Ordering;
};

// TODO(arv): Make all schemas use same hidden class
