import type {Schema, TableSchema} from '../../../zero-types/src/schema.ts';
import type {CRUDMutationRequest} from './crud-mutation-request.ts';
import type {
  DeleteID,
  InsertValue,
  UpdateValue,
  UpsertValue,
} from './custom.ts';

/**
 * The return type of createCRUD - provides CRUD mutation builders for each table.
 */
export type SchemaCRUDMutations<S extends Schema> = {
  readonly [K in keyof S['tables'] & string]: TableCRUDMutations<S, K>;
};

/**
 * CRUD mutation builders for a single table.
 */
export type TableCRUDMutations<
  S extends Schema,
  TTable extends keyof S['tables'] & string,
> = {
  insert: (
    value: InsertValue<S['tables'][TTable]>,
  ) => CRUDMutationRequest<S, TTable, 'insert'>;
  upsert: (
    value: UpsertValue<S['tables'][TTable]>,
  ) => CRUDMutationRequest<S, TTable, 'upsert'>;
  update: (
    value: UpdateValue<S['tables'][TTable]>,
  ) => CRUDMutationRequest<S, TTable, 'update'>;
  delete: (
    id: DeleteID<S['tables'][TTable]>,
  ) => CRUDMutationRequest<S, TTable, 'delete'>;
};

/**
 * Creates CRUD mutation builders for the given schema.
 *
 * Usage:
 * ```ts
 * const crud = createCRUD(schema);
 * tx.mutate(crud.issues.insert({id: '1', title: 'Hello'}));
 * tx.mutate(crud.issues.update({id: '1', title: 'Updated'}));
 * tx.mutate(crud.issues.delete({id: '1'}));
 * ```
 */
export function createCRUD<S extends Schema>(
  schema: S,
): SchemaCRUDMutations<S> {
  const cache = new Map<string, TableCRUDMutations<S, string>>();
  const {tables} = schema;

  function getTableCRUD(
    tableName: string,
  ): TableCRUDMutations<S, string> | undefined {
    const cached = cache.get(tableName);
    if (cached) {
      return cached;
    }

    if (!Object.hasOwn(tables, tableName)) {
      return undefined;
    }

    const tableCRUD = makeTableCRUDMutations<S>(tableName);
    cache.set(tableName, tableCRUD);
    return tableCRUD;
  }

  return new Proxy(tables, {
    get: (_target, prop) => {
      if (typeof prop === 'symbol') {
        return undefined;
      }
      const crud = getTableCRUD(prop);
      if (!crud) {
        throw new Error(`Table ${String(prop)} does not exist in schema`);
      }
      return crud;
    },

    getOwnPropertyDescriptor: (_target, prop) => {
      if (typeof prop === 'symbol') {
        return undefined;
      }
      const value = getTableCRUD(prop);
      if (!value) {
        return undefined;
      }
      const desc = Reflect.getOwnPropertyDescriptor(tables, prop);
      return {...desc, value};
    },
  }) as unknown as SchemaCRUDMutations<S>;
}

function makeTableCRUDMutations<S extends Schema>(
  tableName: string,
): TableCRUDMutations<S, string> {
  // The implementation uses generic TableSchema types, but the return type
  // is schema-specific. The types are compatible at runtime.
  return {
    insert: (value: InsertValue<TableSchema>) => ({
      kind: 'crud' as const,
      table: tableName,
      op: 'insert' as const,
      value,
    }),
    upsert: (value: UpsertValue<TableSchema>) => ({
      kind: 'crud' as const,
      table: tableName,
      op: 'upsert' as const,
      value,
    }),
    update: (value: UpdateValue<TableSchema>) => ({
      kind: 'crud' as const,
      table: tableName,
      op: 'update' as const,
      value,
    }),
    delete: (id: DeleteID<TableSchema>) => ({
      kind: 'crud' as const,
      table: tableName,
      op: 'delete' as const,
      value: id,
    }),
  } as unknown as TableCRUDMutations<S, string>;
}
