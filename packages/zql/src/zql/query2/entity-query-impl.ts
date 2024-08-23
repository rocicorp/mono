/* eslint-disable @typescript-eslint/no-explicit-any */
import {assert} from 'shared/src/asserts.js';
import {AST} from '../ast2/ast.js';
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
import {Host} from '../builder/builder.js';

export function newEntityQuery<
  TSchema extends EntitySchema,
  TReturn extends QueryResultRow[] = [],
>(host: Host, schema: TSchema): EntityQuery<TSchema, TReturn> {
  return new EntityQueryImpl(host, schema);
}

class EntityQueryImpl<
  TSchema extends EntitySchema,
  TReturn extends QueryResultRow[] = [],
  TAs extends string = string,
> implements EntityQuery<TSchema, TReturn, TAs>
{
  readonly #ast: AST;
  readonly #context: Host;
  readonly #schema: TSchema;

  constructor(host: Host, schema: TSchema, ast?: AST | undefined) {
    this.#ast = ast ?? {
      table: schema.table,
    };
    this.#context = host;
    this.#schema = schema;
  }

  #create<
    TSchema extends EntitySchema,
    TReturn extends QueryResultRow[],
    TAs extends string,
  >(host: Host, schema: TSchema, ast: AST): EntityQuery<TSchema, TReturn, TAs> {
    return new EntityQueryImpl(host, schema, ast);
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

  related<
    TRelationship extends keyof TSchema['relationships'],
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    TSub extends EntityQuery<any, any, any>,
  >(
    relationship: TRelationship,
    cb: (
      query: EntityQuery<
        PullSchemaForRelationship<TSchema, TRelationship>,
        [],
        TRelationship & string
      >,
    ) => TSub,
  ): EntityQuery<TSchema, AddSubselect<TSub, TReturn>[], TAs> {
    const related = this.#schema.relationships?.[relationship as string];
    assert(related, 'Invalid relationship');
    const related1 = related;
    const related2 = related;
    if (isFieldRelationship(related1)) {
      const destSchema = resolveSchema(related1.dest.schema);
      return this.#create(this.#context, this.#schema, {
        ...this.#ast,
        related: [
          ...(this.#ast.related ?? []),
          {
            correlation: {
              parentField: related1.source,
              childField: related1.dest.field,
              op: '=',
            },
            subquery: cb(
              this.#create(this.#context, destSchema, {
                table: destSchema.table,
              }),
            ).ast,
          },
        ],
      });
    }

    if (isJunctionRelationship(related2)) {
      const destSchema = resolveSchema(related2.dest.schema);
      const junctionSchema = resolveSchema(related2.junction.schema);
      return this.#create(this.#context, this.#schema, {
        ...this.#ast,
        related: [
          ...(this.#ast.related ?? []),
          {
            correlation: {
              parentField: related2.source,
              childField: related2.junction.sourceField,
              op: '=',
            },
            subquery: {
              table: junctionSchema.table,
              related: [
                {
                  correlation: {
                    parentField: related2.junction.destField,
                    childField: related2.dest.field,
                    op: '=',
                  },
                  subquery: cb(
                    this.#create(this.#context, destSchema, {
                      table: destSchema.table,
                    }),
                  ).ast,
                },
              ],
            },
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
      where: [
        ...(this.#ast.where ?? []),
        {
          type: 'simple',
          op,
          field: field as string,
          value,
        },
      ],
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
