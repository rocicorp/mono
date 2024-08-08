import {
  EntityQuery,
  FromSet,
  newEntityQuery,
} from 'zql/src/zql/query/entity-query.js';
import type {Entity} from 'zql/src/zql/schema/entity-schema.js';
import type {ZqlLiteZeroOptions} from './options.js';
import type {Context as ZQLContext} from 'zql/src/zql/context/context.js';
import {createContext} from './context.js';
import {ZQLite} from './ZQLite.js';
import {
  BaseCRUDMutate,
  EntityCRUDMutate,
  makeBatchCRUDMutate,
  MakeCRUDMutate,
  Update,
} from 'zero-client/src/client/crud.js';
import type {CRUDOp, CRUDOpKind} from 'zero-protocol/src/push.js';
import type {Database} from 'better-sqlite3';
import type {EntityID} from 'zero-protocol/src/entity.js';
import {
  QueryDefs,
  MakeEntityQueriesFromQueryDefs,
  NoRelations,
} from 'zero-client/src/client/zero.js';
import {QueryParseDefs} from 'zero-client/src/client/options.js';

export class ZqlLiteZero<QD extends QueryDefs> {
  readonly zqlContext: ZQLContext;
  readonly query: MakeEntityQueriesFromQueryDefs<QD>;
  readonly zqlLite: ZQLite;
  readonly mutate: MakeCRUDMutate<QD>;
  db: Database;

  constructor(options: ZqlLiteZeroOptions<QD>) {
    const {queries = {} as QueryParseDefs<QD>, db} = options;
    this.db = db;
    this.zqlLite = new ZQLite(db);
    this.zqlContext = createContext(this.zqlLite, db);
    this.query = this.#registerQueries(queries);
    this.mutate = this.#makeCRUDMutate<QD>(queries, db);
  }

  #registerQueries(
    queryDefs: QueryParseDefs<QD>,
  ): MakeEntityQueriesFromQueryDefs<QD> {
    const rv = {} as Record<string, EntityQuery<FromSet, NoRelations, []>>;
    const context = this.zqlContext;
    // Not using parse yet
    for (const name of Object.keys(queryDefs)) {
      rv[name] = newEntityQuery(context, name);
    }
    return rv as MakeEntityQueriesFromQueryDefs<QD>;
  }

  #makeCRUDMutate<QD extends QueryDefs>(
    queries: QueryParseDefs<QD>,
    db: Database,
  ): MakeCRUDMutate<QD> {
    let inBatch = false;

    const mutate = async <R>(
      body: (m: BaseCRUDMutate<QD>) => R,
    ): Promise<R> => {
      if (inBatch) {
        throw new Error('Cannot call mutate inside a batch');
      }
      inBatch = true;

      try {
        const ops: CRUDOp[] = [];
        const m = {} as Record<string, unknown>;
        for (const name of Object.keys(queries)) {
          m[name] = makeBatchCRUDMutate(name, ops);
        }

        const rv = await body(m as BaseCRUDMutate<QD>);
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

    for (const name of Object.keys(queries)) {
      (mutate as unknown as Record<string, EntityCRUDMutate<Entity>>)[name] =
        this.makeEntityCRUDMutate(name, db, assertNotInBatch);
    }
    return mutate as MakeCRUDMutate<QD>;
  }

  makeEntityCRUDMutate<E extends Entity>(
    entityType: string,
    db: Database,
    assertNotInBatch: (entityType: string, op: CRUDOpKind) => void,
  ): EntityCRUDMutate<E> {
    return {
      create: async (value: E) => {
        assertNotInBatch(entityType, 'create');
        const existingEntity = await db
          .prepare(`SELECT * FROM ${entityType} WHERE id = ?`)
          .get(value.id);
        if (existingEntity) return;
        //console.log('Adding', value);
        await this.zqlContext.getSource(entityType).add(value);
      },
      set: async (value: E) => {
        assertNotInBatch(entityType, 'set');
        await this.zqlContext.getSource(entityType).add(value);
      },
      update: async (value: Update<E>) => {
        assertNotInBatch(entityType, 'update');
        const existingEntity = await db
          .prepare(`SELECT * FROM ${entityType} WHERE id = ?`)
          .get(value.id);
        if (!existingEntity)
          throw new Error(`Entity with id ${value.id} not found`);
        const mergedValue = {...existingEntity, ...value};
        await this.zqlContext.getSource(entityType).delete(existingEntity);
        await this.zqlContext.getSource(entityType).add(mergedValue);
      },
      delete: async (id: EntityID) => {
        assertNotInBatch(entityType, 'delete');
        const existingEntity = await db
          .prepare(`SELECT * FROM ${entityType} WHERE id = ?`)
          .get(id);
        if (!existingEntity) throw new Error(`Entity with id ${id} not found`);
        await this.zqlContext.getSource(entityType).delete(existingEntity);
      },
    };
  }
}
