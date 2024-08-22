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
  return new UnmooredEntityQuery(context, schema);
}

abstract class AbstractEntityQuery<
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

  protected abstract _create<
    TSchema extends EntitySchema,
    TReturn extends QueryResultRow[],
    TAs extends string,
  >(
    context: Context,
    schema: TSchema,
    ast: AST,
  ): EntityQuery<TSchema, TReturn, TAs>;

  get ast() {
    return this.#ast;
  }

  select<TFields extends Selector<TSchema>[]>(
    ..._fields: TFields
  ): EntityQuery<TSchema, AddSelections<TSchema, TFields, TReturn>[], TAs> {
    // we return all columns for now so we ignore the selection set and only use it for type inference
    return this._create(this.#context, this.#schema, this.#ast);
  }

  run(): MakeHumanReadable<TReturn> {
    throw new Error('Method not implemented.');
  }

  sub<TSub extends EntityQuery<any, any, any>>(
    cb: (query: EntityQuery<TSchema>) => TSub,
  ): EntityQuery<TSchema, AddSubselect<TSub, TReturn>[], TAs> {
    const subquery = cb(
      new AnchoredEntityQuery(this.#context, this.#schema, {
        type: 'anchored',
      }),
    );
    return this._create(this.#context, this.#schema, {
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
      return this._create(this.#context, this.#schema, {
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
      return this._create(this.#context, this.#schema, {
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
    return this._create(this.#context, this.#schema, {
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
    return this._create(this.#context, this.#schema, {
      ...this.#ast,
      alias,
    });
  }
}

class UnmooredEntityQuery<
  TSchema extends EntitySchema,
  TReturn extends QueryResultRow[] = [],
  TAs extends string = string,
> extends AbstractEntityQuery<TSchema, TReturn, TAs> {
  constructor(context: Context, schema: TSchema, ast?: AST | undefined) {
    super(context, schema, ast);
  }

  protected _create<
    TSchema extends EntitySchema,
    TReturn extends QueryResultRow[],
    TAs extends string,
  >(
    context: Context,
    schema: TSchema,
    ast: AST,
  ): EntityQuery<TSchema, TReturn, TAs> {
    return new UnmooredEntityQuery(context, schema, ast);
  }
}

class AnchoredEntityQuery<
  TSchema extends EntitySchema,
  TReturn extends QueryResultRow[] = [],
  TAs extends string = string,
> extends AbstractEntityQuery<TSchema, TReturn, TAs> {
  constructor(context: Context, schema: TSchema, ast: AST) {
    super(context, schema, ast);
  }

  protected _create<
    TSchema extends EntitySchema,
    TReturn extends QueryResultRow[],
    TAs extends string,
  >(
    context: Context,
    schema: TSchema,
    ast: AST,
  ): EntityQuery<TSchema, TReturn, TAs> {
    return new AnchoredEntityQuery(context, schema, ast);
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

/**
 * Need unmoored and anchored types.
 * `sub` passes the `anchored` type to the callback.
 * Anchored type is forever anchored?
 * Related doing different things.
 *
 * The first `related` for an anchored query
 * puts the thing in the correlation.
 *
 * Well do we need the anchored type?
 * We can just return the normal type and pull out
 * the `related` correctly if it is used in a subquery
 * position.
 *
 * Well... not if it is an uncorrelated subquery.
 *
 * We can only pull the first related if it is anchored.
 *
 * If it is anchored and there is no related?
 * Scan the `wheres` for the correlation.
 */
