import type {Ordering} from '../../../zero-protocol/src/ast.js';
import type {Row} from '../../../zero-protocol/src/data.js';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.js';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.js';

/**
 * Information about the nodes output by an operator.
 */
export type SourceSchema = {
  readonly tableName: string;
  readonly columns: Record<string, SchemaValue>;
  readonly primaryKey: PrimaryKey;
  readonly relationships: {[key: string]: SourceSchema};
  readonly isHidden: boolean;
  readonly compareRows: (r1: Row, r2: Row) => number;
  readonly sort: Ordering;
};
