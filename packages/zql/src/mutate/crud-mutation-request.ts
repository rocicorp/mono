import type {Schema, TableSchema} from '../../../zero-types/src/schema.ts';
import type {
  DeleteID,
  InsertValue,
  UpdateValue,
  UpsertValue,
} from './custom.ts';

export type CRUDOp = 'insert' | 'upsert' | 'update' | 'delete';

export type CRUDMutationRequest<
  S extends Schema = Schema,
  TTable extends keyof S['tables'] & string = keyof S['tables'] & string,
  TOp extends CRUDOp = CRUDOp,
> = {
  readonly kind: 'crud';
  readonly table: TTable;
  readonly op: TOp;
  readonly value: CRUDValue<S['tables'][TTable], TOp>;
};

type CRUDValue<T extends TableSchema, TOp extends CRUDOp> = TOp extends 'insert'
  ? InsertValue<T>
  : TOp extends 'upsert'
    ? UpsertValue<T>
    : TOp extends 'update'
      ? UpdateValue<T>
      : TOp extends 'delete'
        ? DeleteID<T>
        : never;

export function isCRUDMutationRequest(
  value: unknown,
): value is CRUDMutationRequest {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as CRUDMutationRequest).kind === 'crud'
  );
}
