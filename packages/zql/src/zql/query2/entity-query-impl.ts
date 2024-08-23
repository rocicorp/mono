/* eslint-disable @typescript-eslint/no-explicit-any */
import {assert} from '../../../../shared/src/asserts.js';
import {AST} from '../ast2/ast.js';
import {Context} from '../context/context.js';
import {
  AddSelections,
  AddSubselect,
  EntityQuery,
  GetFieldType,
  MakeHumanReadable,
  Operator,
  QueryResultRow,
  Selector,
} from './entity-query.js';
import {
  EntitySchema,
  isFieldRelationship,
  isJunctionRelationship,
  Lazy,
  PullSchemaForRelationship,
} from './schema.js';

export function newEntityQuery<
  TSchema extends EntitySchema,
  TReturn extends QueryResultRow[] = [],
>(context: Context, schema: TSchema): EntityQuery<TSchema, TReturn> {
  return new EntityQueryImpl(context, schema);
}

class EntityQueryImpl<
  TSchema extends EntitySchema,
  TReturn extends QueryResultRow[] = [],
  TAs extends string = string,
> implements EntityQuery<TSchema, TReturn, TAs>
{
  readonly #ast: AST;
  readonly #context: Context;
  readonly #schema: TSchema;

  constructor(context: Context, schema: TSchema, ast?: AST | undefined) {
    this.#ast = ast ?? {
      type: 'unmoored',
      table: schema.table,
    };
    this.#context = context;
    this.#schema = schema;
  }

  #create<
    TSchema extends EntitySchema,
    TReturn extends QueryResultRow[],
    TAs extends string,
  >(
    context: Context,
    schema: TSchema,
    ast: AST,
  ): EntityQuery<TSchema, TReturn, TAs> {
    return new EntityQueryImpl(context, schema, ast);
  }

  get ast() {
    return this.#ast;
  }

  select<TFields extends Selector<TSchema>[]>(
    ..._fields: TFields
  ): EntityQuery<TSchema, AddSelections<TSchema, TFields, TReturn>[], TAs> {
    // we return all columns for now so we ignore the selection set and only use it for type inference
    return this.#create(this.#context, this.#schema, this.#ast);
  }

  run(): MakeHumanReadable<TReturn> {
    throw new Error('Method not implemented.');
  }

  sub<TSub extends EntityQuery<any, any, any>>(
    cb: (query: EntityQuery<TSchema>) => TSub,
  ): EntityQuery<TSchema, AddSubselect<TSub, TReturn>[], TAs> {
    const subquery = cb(
      this.#create(this.#context, this.#schema, {
        type: 'anchored',
      }),
    );
    return this.#create(this.#context, this.#schema, {
      ...this.#ast,
      subqueries: [...(this.#ast.subqueries ?? []), subquery.ast],
    });
  }

  related<TRelationship extends keyof TSchema['relationships']>(
    relationship: TRelationship,
  ): EntityQuery<
    PullSchemaForRelationship<TSchema, TRelationship>,
    [],
    TRelationship & string
  > {
    const related = this.#schema.relationships?.[relationship as string];
    assert(related, 'Invalid relationship');
    const related1 = related;
    const related2 = related;
    if (isFieldRelationship(related1)) {
      return this.#create(this.#context, this.#schema, {
        ...this.#ast,
        related: [
          ...(this.#ast.related ?? []),
          {
            sourceField: related1.source,
            destField: related1.dest.field,
            destTable: resolveSchema(related1.dest.schema).table,
          },
        ],
      });
    }

    if (isJunctionRelationship(related2)) {
      return this.#create(this.#context, this.#schema, {
        ...this.#ast,
        related: [
          ...(this.#ast.related ?? []),
          {
            sourceField: related2.source,
            junctionTable: resolveSchema(related2.junction.schema).table,
            junctionSourceField: related2.junction.sourceField,
            junctionDestField: related2.junction.destField,
            destField: related.dest.field,
            destTable: resolveSchema(related.dest.schema).table,
          },
        ],
      });
    }

    throw new Error(`Invalid relationship ${relationship as string}`);
  }

  where<TSelector extends Selector<TSchema>>(
    field: TSelector,
    op: Operator,
    value: Exclude<GetFieldType<TSchema, TSelector>, null | undefined>,
  ): EntityQuery<TSchema, TReturn, TAs> {
    return this.#create(this.#context, this.#schema, {
      ...this.#ast,
      where: {
        type: 'simple',
        op,
        field: field as string,
        value,
      },
    });
  }

  as<TAs2 extends string>(alias: TAs2): EntityQuery<TSchema, TReturn, TAs2> {
    return this.#create(this.#context, this.#schema, {
      ...this.#ast,
      alias,
    });
  }
}

function resolveSchema(
  maybeSchema: EntitySchema | Lazy<EntitySchema>,
): EntitySchema {
  if (typeof maybeSchema === 'function') {
    return maybeSchema();
  }

  return maybeSchema;
}
