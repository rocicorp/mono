import {Row} from '../ivm2/data.js';

type Entity = Row;

/**
 * The type that can be passed into `select()`. A selector
 * references a field on an entity.
 */
type Selector<E extends Entity> = keyof E;

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
type AddSelections<
  TEntity extends Entity,
  TSelections extends Selector<TEntity>[],
  TReturn extends QueryResultRow[],
> = {
  entity: {[K in TSelections[number]]: TEntity[K]};
  subselects: TReturn[number]['subselects'];
};

/**
 * Just like `AddSelections` but adds a subselect
 * to the return type (TReturn).
 */
type AddSubselect<
  TSubquery extends EntityQuery<Entity>,
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
type PickSubselect<TSubquery extends EntityQuery<Entity>> = {
  [K in TSubquery extends EntityQuery<Entity, QueryResultRow[], infer TAs>
    ? TAs
    : never]: TSubquery extends EntityQuery<Entity, infer TSubreturn>
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
type QueryResultRow = {
  entity: Partial<Entity>;
  subselects: Record<string, QueryResultRow[]> | undefined;
};

export interface EntityQuery<
  TEntity extends Entity,
  TReturn extends QueryResultRow[] = [],
  TAs extends string = string,
> {
  select<TFields extends Selector<TEntity>[]>(
    ...x: TFields
  ): EntityQuery<TEntity, AddSelections<TEntity, TFields, TReturn>[], TAs>;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  sub<TSub extends EntityQuery<Entity, any, any>>(
    cb: (query: EntityQuery<TEntity>) => TSub,
  ): EntityQuery<TEntity, AddSubselect<TSub, TReturn>[], TAs>;

  as<TAs2 extends string>(as: TAs2): EntityQuery<TEntity, TReturn, TAs2>;

  run(): MakeHumanReadable<TReturn>;
}
