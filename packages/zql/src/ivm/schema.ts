import type {Ordering, System} from '../../../zero-protocol/src/ast.ts';
import type {PrimaryKey} from '../../../zql/src/ivm/constraint.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import type {Comparator} from './data.ts';

/**
 * Information about the nodes output by an operator.
 */
export type SourceSchema = {
  readonly tableName: string;
  readonly columns: Record<string, SchemaValue>;
  readonly primaryKey: PrimaryKey;
  readonly relationships: {[key: string]: SourceSchema};
  readonly isHidden: boolean;
  // The system responsible for the presence of this node in the query.
  // When the `permissions` system is responsible for adding a node,
  // we should not sync the data coming out of it to the client.
  // Permission rules can access anything so data coming out of them may not
  // be visible to the user.
  // E.g., maybe a user can see an object because of an entry in the session table.
  // We should not sync the data from the session table to the client.
  readonly system: System;
  readonly compareRows: Comparator;
  readonly sort: Ordering;
};
