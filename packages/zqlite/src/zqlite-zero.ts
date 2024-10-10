import {must} from '../../shared/src/must.js';
import {ZeroContext} from '../../zero-client/src/client/context.js';
import {
  makeBatchCRUDMutate,
  type BaseCRUDMutate,
  type CreateValue,
  type DeleteID,
  type MakeCRUDMutate,
  type RowCRUDMutate,
  type SetValue,
  type UpdateValue,
} from '../../zero-client/src/client/crud.js';
import * as zeroJs from '../../zero-client/src/client/zero.js';
import type {Query} from '../../zero-client/src/mod.js';
import type {PrimaryKey} from '../../zero-protocol/src/primary-key.js';
import type {CRUDOp, CRUDOpKind} from '../../zero-protocol/src/push.js';
import type {Row} from '../../zql/src/zql/ivm/data.js';
import {newQuery} from '../../zql/src/zql/query/query-impl.js';
import type {TableSchema} from '../../zql/src/zql/query/schema.js';
import type {Database} from './db.js';
import type {ZQLiteZeroOptions} from './options.js';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TODO = any;
export class ZQLiteZero<S extends zeroJs.Schema> {
  readonly zeroContext: ZeroContext;
  readonly query: zeroJs.MakeEntityQueriesFromSchema<S>;
  readonly mutate: MakeCRUDMutate<S>;
  db: Database;

  constructor(options: ZQLiteZeroOptions<S>) {
    const {schema, db} = options;
    this.db = db;
    this.zeroContext = {} as TODO;
    this.query = this.#registerQueries(schema);
    this.mutate = this.#makeCRUDMutate<S>(schema, db);
  }

  #registerQueries(schema: S): zeroJs.MakeEntityQueriesFromSchema<S> {
    const rv = {} as Record<string, Query<TableSchema>>;
    const context = this.zeroContext;
    // Not using parse yet
    for (const [name, table] of Object.entries(schema.tables)) {
      rv[name] = newQuery(context, table);
    }
    return rv as zeroJs.MakeEntityQueriesFromSchema<S>;
  }

  #makeCRUDMutate<S extends zeroJs.Schema>(
    schema: S,
    db: Database,
  ): MakeCRUDMutate<S> {
    let inBatch = false;

    const mutate = async <R>(body: (m: BaseCRUDMutate<S>) => R): Promise<R> => {
      if (inBatch) {
        throw new Error('Cannot call mutate inside a batch');
      }
      inBatch = true;

      try {
        const ops: CRUDOp[] = [];
        const m = {} as Record<string, unknown>;
        for (const name of Object.keys(schema.tables)) {
          m[name] = makeBatchCRUDMutate(name, schema, ops);
        }

        const rv = await body(m as BaseCRUDMutate<S>);
        return rv;
      } finally {
        inBatch = false;
      }
    };

    const assertNotInBatch = (entityType: string, op: CRUDOpKind) => {
      if (inBatch) {
        throw new Error(
          `Cannot call mutate.${entityType}.${op} inside a batch`,
        );
      }
    };

    for (const name of Object.keys(schema.tables)) {
      (mutate as unknown as Record<string, RowCRUDMutate<Row, PrimaryKey>>)[
        name
      ] = this.makeEntityCRUDMutate(name, db, assertNotInBatch);
    }
    return mutate as MakeCRUDMutate<S>;
  }

  makeEntityCRUDMutate<R extends Row, PK extends PrimaryKey>(
    entityType: string,
    db: Database,
    assertNotInBatch: (entityType: string, op: CRUDOpKind) => void,
  ): RowCRUDMutate<R, PK> {
    return {
      create: async (value: CreateValue<R, PK>) => {
        assertNotInBatch(entityType, 'create');
        const existingEntity = await db
          .prepare(`SELECT * FROM ${entityType} WHERE id = ?`)
          .get(value.id);
        if (existingEntity) return;
        //console.log('Adding', value);
        await must(this.zeroContext.getSource(entityType)).push({
          type: 'add',
          row: value,
        });
      },
      set: async (value: SetValue<R, PK>) => {
        assertNotInBatch(entityType, 'set');
        await must(this.zeroContext.getSource(entityType)).push({
          type: 'add',
          row: value,
        });
      },
      update: async (value: UpdateValue<R, PK>) => {
        assertNotInBatch(entityType, 'update');
        const existingEntity = await db
          .prepare(`SELECT * FROM ${entityType} WHERE id = ?`)
          .get<Row>(value.id);
        if (!existingEntity)
          throw new Error(`Entity with id ${value.id} not found`);
        const mergedValue = {...existingEntity, ...value};
        await must(this.zeroContext.getSource(entityType)).push({
          type: 'remove',
          row: existingEntity,
        });
        await must(this.zeroContext.getSource(entityType)).push({
          type: 'add',
          row: mergedValue,
        });
      },
      delete: async (id: DeleteID<R, PK>) => {
        assertNotInBatch(entityType, 'delete');
        // TODO: Remove the useless awaits here and elsewhere.
        const existingEntity = await db
          .prepare(`SELECT * FROM ${entityType} WHERE id = ?`)
          .get<Row>(id);
        if (!existingEntity) throw new Error(`Entity with id ${id} not found`);
        await must(this.zeroContext.getSource(entityType)).push({
          type: 'remove',
          row: existingEntity,
        });
      },
    };
  }
}
