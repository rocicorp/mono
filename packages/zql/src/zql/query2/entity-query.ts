import {AST} from '../ast2/ast.js';
import {Row} from '../ivm2/data.js';
import {
  EntitySchema,
  PullSchemaForRelationship,
  SchemaValue,
} from './schema.js';

type Entity = Row;

/**
 * The type that can be passed into `select()`. A selector
 * references a field on an entity.
 */
export type Selector<E extends EntitySchema> = keyof E['fields'];

/**
 * Have you ever noticed that when you hover over Types in TypeScript, it shows
 * Pick<Omit<T, K>, K>? Rather than the final object structure after picking and omitting?
 * Or any time you use a type alias.
 *
 * MakeHumanReadable collapses the type aliases into their final form.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
export type MakeHumanReadable<T> = {} & {
  readonly [P in keyof T]: T[P] extends string ? T[P] : MakeHumanReadable<T[P]>;
};

/**
 * Given a schema value, return the TypeScript type.
 *
 * This allows us to create the correct return type for a
 * query that has a selection.
 */
type SchemaValueToTSType<T extends SchemaValue> =
  | (T extends {type: 'string'}
      ? string
      : T extends {type: 'number'}
      ? number
      : T extends {type: 'boolean'}
      ? boolean
      : T extends {type: 'null'}
      ? null
      : never)
  | (T extends {optional: true} ? undefined : never);

export type GetFieldType<
  TSchema extends EntitySchema,
  TField extends keyof TSchema['fields'],
> = SchemaValueToTSType<TSchema['fields'][TField]>;

export type EntitySchemaToEntity<T extends EntitySchema> = {
  [K in keyof T['fields']]: SchemaValueToTSType<T['fields'][K]>;
};

/**
 * A query can have:
 * 1. Selections and
 * 2. Subqueries
 *
 * The composition of these two yields the return type
 * of the query.
 *
 * This takes a return type of a query (TReturn), an entity type (TEntity),
 * and a list of selections (TSelections) made against that entity,
 * returning a new return type with the selections added.
 *
 * `.select('foo')` would add `foo` to `TReturn`.
 */
export type AddSelections<
  TSchema extends EntitySchema,
  TSelections extends Selector<TSchema>[],
  TReturn extends QueryResultRow[],
> = {
  entity: {
    [K in TSelections[number]]: SchemaValueToTSType<TSchema['fields'][K]>;
  };
  subselects: TReturn[number]['subselects'];
};

/**
 * Just like `AddSelections` but adds a subselect
 * to the return type (TReturn).
 */
export type AddSubselect<
  TSubquery extends EntityQuery<EntitySchema>,
  TReturn extends QueryResultRow[],
> = {
  entity: TReturn[number]['entity'];
  subselects: TReturn[number]['subselects'] extends never
    ? PickSubselect<TSubquery>
    : PickSubselect<TSubquery> & TReturn[number]['subselects'];
};

/**
 * Subselects have aliases which is how they're inserted
 * into the tree of values. This function takes a subquery
 * and infers its alias to be used in the return type.
 *
 * `sub(query => query.select('foo').as('bar'))` would
 * return `{bar: {foo: string}}`.
 */
type PickSubselect<TSubquery extends EntityQuery<EntitySchema>> = {
  [K in TSubquery extends EntityQuery<EntitySchema, QueryResultRow[], infer TAs>
    ? TAs
    : never]: TSubquery extends EntityQuery<EntitySchema, infer TSubreturn>
    ? TSubreturn
    : never;
};

/**
 * The result of a ZQL query.
 *
 * Represents a tree of entities and subselects.
 *
 * ```ts
 * z.issue.select('title').sub(q => q.related('comments').select('text')).run();
 * ```
 *
 * would return:
 *
 * ```
 * [{
 *  entity: {title: 'foo'},
 *  subselects: {
 *    comments: [
 *      {
 *        entity: {text: 'bar'},
 *        subselects: undefined,
 *      },
 *    ],
 * }]
 * ```
 */
export type QueryResultRow = {
  entity: Partial<Entity>;
  subselects: Record<string, QueryResultRow[]> | undefined;
};

export type Operator = '=' | '!=' | '<' | '<=' | '>' | '>=';

export interface EntityQuery<
  TSchema extends EntitySchema,
  TReturn extends QueryResultRow[] = [],
  TAs extends string = string,
> {
  readonly ast: AST;

  select<TFields extends Selector<TSchema>[]>(
    ...x: TFields
  ): EntityQuery<TSchema, AddSelections<TSchema, TFields, TReturn>[], TAs>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  sub<TSub extends EntityQuery<any, any, any>>(
    // If we only want to allow `related` : Pick<EntityQuery<TSchema>, 'related'>
    cb: (query: EntityQuery<TSchema>) => TSub,
  ): EntityQuery<TSchema, AddSubselect<TSub, TReturn>[], TAs>;

  as<TAs2 extends string>(as: TAs2): EntityQuery<TSchema, TReturn, TAs2>;

  related<TRelationship extends keyof TSchema['relationships']>(
    relationship: TRelationship,
  ): EntityQuery<
    PullSchemaForRelationship<TSchema, TRelationship>,
    [],
    TRelationship & string
  >;

  where<TSelector extends Selector<TSchema>>(
    field: TSelector,
    op: Operator,
    value: GetFieldType<TSchema, TSelector>,
  ): EntityQuery<TSchema, TReturn, TAs>;

  run(): MakeHumanReadable<TReturn>;
}
