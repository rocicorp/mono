// oxlint-disable no-explicit-any
import {assert} from '../../../shared/src/asserts.ts';
import type {
  ReadonlyJSONObject,
  ReadonlyJSONValue,
} from '../../../shared/src/json.ts';
import {
  type AST,
  type CompoundKey,
  type Condition,
  type NormalizedAST,
  type Parameter,
  type LiteralValue,
  type SimpleCondition,
  type SimpleOperator,
  type System,
  insertRelated,
  normalizeAST,
  normalizeCondition,
  normalizedRelated,
  SUBQ_PREFIX,
  tableAST,
} from '../../../zero-protocol/src/ast.ts';
import {hashOfQueryInternals} from '../../../zero-protocol/src/query-hash-visitor.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {NotImplementedError} from '../error.ts';
import {defaultFormat} from '../ivm/default-format.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import {
  type ExpressionFactory,
  ExpressionBuilder,
  and,
  cmp,
  simplifyCondition,
} from './expression.ts';
import type {CustomQueryID} from './named.ts';
import {type QueryInternals, queryInternalsTag} from './query-internals.ts';
import {
  Transitions,
  type Delta,
  type TransitionValue,
} from './query-transitions.ts';
import type {
  AnyQuery,
  ExistsOptions,
  GetFilterType,
  HumanReadable,
  PreloadOptions,
  PullRow,
  Query,
  RunOptions,
} from './query.ts';
import type {TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

type GetFilterTypeAny = GetFilterType<any, any, any>;

type NewQueryFunction<TSchema extends Schema> = <
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  this: unknown,
  tableName: TTable,
  ast: NormalizedAST,
  format: Format,
  customQueryID: CustomQueryID | undefined,
  currentJunction: string | undefined,
) => QueryImpl<TTable, TSchema, TReturn>;

export function newQuery<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
>(schema: TSchema, table: TTable): Query<TTable, TSchema> {
  return newQueryInternal(
    schema,
    table,
    tableAST(table),
    defaultFormat,
    'client',
  );
}

export function newQueryImpl<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
>(
  schema: TSchema,
  tableName: TTable,
  ast: AST,
  format: Format,
  system: System,
): QueryImpl<TTable, TSchema, TReturn> {
  // This is the entry point for ASTs that were not built by QueryImpl, so
  // normalize it here to establish the invariant that #ast is normalized.
  return newQueryInternal(schema, tableName, normalizeAST(ast), format, system);
}

/**
 * Root queries are interned per schema so that two `createBuilder(schema)` calls
 * hand out the same starting point, and therefore share the whole transition
 * tree below it. Only cleared entries accumulate here, at most one per
 * system/table pair, so no sweeping is needed.
 */
const rootsBySchema = new WeakMap<
  Schema,
  Map<string, WeakRef<QueryImpl<any, any, any>>>
>();

/**
 * Whether this is the AST `tableAST(tableName)` produces -- the whole table,
 * nothing else. A normalized AST carries every field, so this cannot be a key
 * count; each one has to be checked.
 */
function isRootAST(ast: NormalizedAST, tableName: string): boolean {
  return (
    ast.table === tableName &&
    ast.schema === undefined &&
    ast.alias === undefined &&
    ast.where === undefined &&
    ast.related === undefined &&
    ast.start === undefined &&
    ast.limit === undefined &&
    ast.orderBy === undefined
  );
}

function newQueryInternal<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
>(
  schema: TSchema,
  tableName: TTable,
  ast: NormalizedAST,
  format: Format,
  system: System,
): QueryImpl<TTable, TSchema, TReturn> {
  // Callers that hand in a pre-built AST -- benchmarks and the integration-test
  // harnesses -- are not roots and are not interned.
  const interned = format === defaultFormat && isRootAST(ast, tableName);
  const key = `${system}:${tableName}`;
  let roots: Map<string, WeakRef<QueryImpl<any, any, any>>> | undefined;

  if (interned) {
    roots = rootsBySchema.get(schema);
    const existing = roots?.get(key)?.deref();
    if (existing) {
      return existing as QueryImpl<TTable, TSchema, TReturn>;
    }
  }

  const inner: NewQueryFunction<TSchema> = (
    tableName,
    ast,
    format,
    customQueryID,
    currentJunction,
  ) =>
    new QueryImpl(
      schema,
      tableName,
      ast,
      format,
      system,
      customQueryID,
      currentJunction,
      inner,
    );

  const created = inner<TTable, TReturn>(
    tableName,
    ast,
    format,
    undefined,
    undefined,
  );

  if (interned) {
    if (!roots) {
      roots = new Map();
      rootsBySchema.set(schema, roots);
    }
    roots.set(key, new WeakRef(created));
  }

  return created;
}

/**
 * The AST of a QueryImpl is always normalized. Each step builds its AST in
 * normalized form rather than normalizing afterwards: a normalized AST has
 * every field, so spreading one and replacing a field keeps the field order
 * intact, and only the field a step actually changes needs normalizing (the
 * conditions of a `where`, the position of a new `related`). The ASTs that
 * come in from the outside are normalized by {@link newQueryImpl}, which is
 * what the `NormalizedAST` of the AST it holds stands for.
 */
export class QueryImpl<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn = PullRow<TTable, TSchema>,
>
  implements
    Query<TTable, TSchema, TReturn>,
    QueryInternals<TTable, TSchema, TReturn>
{
  readonly [queryInternalsTag] = true;

  readonly #schema: TSchema;
  readonly #tableName: TTable;
  readonly #ast: NormalizedAST;
  readonly format: Format;
  #hash: string = '';
  readonly #system: System;
  readonly #currentJunction: string | undefined;
  readonly customQueryID: CustomQueryID | undefined;
  readonly #newQuery: NewQueryFunction<TSchema>;

  /**
   * The query this one was derived from, held *strongly*, and the queries
   * derived from this one, held *weakly*. This is the arrangement V8 uses for
   * hidden-class transitions, and it is what makes interning stable: as long as
   * a query is reachable its whole ancestor spine is too, so re-deriving it from
   * the root yields the same instance. See `query-transitions.ts`.
   */
  #parent: QueryImpl<any, any, any> | undefined;
  #transitions: Transitions<QueryImpl<any, any, any>> | undefined;

  /**
   * Memoized because a query is interned: the same node serves every rebuild of
   * a chain, so building this once rather than per `where(fn)` call takes the
   * whole thing -- the builder and the bound `#exists` -- off the repeat path.
   */
  #expressionBuilder: ExpressionBuilder<TTable, TSchema> | undefined;

  /**
   * The query this one was derived from, or `undefined` for a root query.
   *
   * @internal Exposed for tests and debugging. The field behind it exists to
   * keep the ancestor spine alive, not to be navigated.
   */
  get derivedFrom(): QueryImpl<any, any, any> | undefined {
    return this.#parent;
  }

  /** @internal Exposed so tests can assert what is retained strongly. */
  get transitionsForTesting():
    | Transitions<QueryImpl<any, any, any>>
    | undefined {
    return this.#transitions;
  }

  constructor(
    schema: TSchema,
    tableName: TTable,
    ast: NormalizedAST,
    format: Format,
    system: System,
    customQueryID: CustomQueryID | undefined,
    currentJunction: string | undefined,
    newQuery: NewQueryFunction<TSchema>,
  ) {
    this.#schema = schema;
    this.#tableName = tableName;
    this.#ast = ast;
    this.format = format;
    this.#system = system;
    this.#currentJunction = currentJunction;
    this.customQueryID = customQueryID;
    this.#newQuery = newQuery;
  }

  /**
   * Interns a transition whose key space is finite and schema-derived; see
   * {@linkcode Transitions.storeBounded}. Used for the base sub-query handed to
   * a `related`/`exists` callback, which is determined entirely by the
   * relationship name.
   */
  #deriveBounded<T>(key: string, build: () => T): T {
    const existing = this.#transitions?.lookupBounded(key);
    if (existing !== undefined) {
      return existing as T;
    }
    return this.#record(key, undefined, undefined, build(), true);
  }

  /**
   * Interns the result of applying a builder operation to this query.
   *
   * `key` names the operation and `value` carries its varying argument, kept
   * apart so the key stays a string V8 has already hashed. When the two do not
   * settle it, `delta` carries the rest and is compared with `deepEqual`. Only
   * the delta is compared, never the whole AST: this query is already canonical,
   * so two of its children are equal exactly when the operations that produced
   * them are.
   *
   * `T` is deliberately unconstrained so that the contextual return type at each
   * call site still drives `#newQuery`'s inference, exactly as it did when the
   * call sites returned `this.#newQuery(...)` directly.
   */
  #derive<T>(
    key: string,
    value: TransitionValue,
    delta: Delta,
    build: () => T,
  ): T {
    const existing = this.#transitions?.lookup(key, value, delta);
    if (existing !== undefined) {
      return existing as T;
    }
    return this.#record(key, value, delta, build(), false);
  }

  #record<T>(
    key: string,
    value: TransitionValue,
    delta: Delta,
    created: T,
    bounded: boolean,
  ): T {
    const q = created as unknown as QueryImpl<any, any, any>;
    q.#parent = this;
    const transitions = (this.#transitions ??= new Transitions());
    if (bounded) {
      transitions.storeBounded(key, q);
    } else {
      transitions.store(key, value, delta, q);
    }
    return created;
  }

  run(_options?: RunOptions): Promise<HumanReadable<TReturn>> {
    throwQueryNotRunnable();
  }

  preload(_options?: PreloadOptions): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    throwQueryNotRunnable();
  }

  materialize(ttl?: TTL): TypedView<HumanReadable<TReturn>>;
  materialize<T>(
    factory: ViewFactory<TTable, TSchema, TReturn, T>,
    ttl?: TTL,
  ): T;
  materialize<T>(
    _factoryOrTTL?: ViewFactory<TTable, TSchema, TReturn, T> | TTL,
    _ttl?: TTL,
  ): T | TypedView<HumanReadable<TReturn>> {
    throwQueryNotRunnable();
  }

  nameAndArgs(
    name: string,
    args: ReadonlyArray<ReadonlyJSONValue>,
  ): Query<TTable, TSchema, TReturn> {
    return this.#derive('nameAndArgs', name, args, () =>
      this.#newQuery(
        this.#tableName,
        this.#ast,
        this.format,
        {
          name,
          args,
        },
        this.#currentJunction,
      ),
    );
  }

  hash(): string {
    if (!this.#hash) {
      this.#hash = hashOfQueryInternals(
        this.#ast,
        this.format,
        this.#system,
        this.#currentJunction,
        this.customQueryID?.name,
        this.customQueryID?.args,
      );
    }
    return this.#hash;
  }

  one(): Query<TTable, TSchema, TReturn | undefined> {
    return this.#derive('one', undefined, undefined, () =>
      this.#newQuery(
        this.#tableName,
        {...this.#ast, limit: 1},
        {
          ...this.format,
          singular: true,
        },
        this.customQueryID,
        this.#currentJunction,
      ),
    );
  }

  whereExists(
    relationship: string,
    cbOrOptions?: ((q: AnyQuery) => AnyQuery) | ExistsOptions,
    options?: ExistsOptions,
  ): Query<TTable, TSchema, TReturn> {
    const cb = typeof cbOrOptions === 'function' ? cbOrOptions : undefined;
    const opts = typeof cbOrOptions === 'function' ? options : cbOrOptions;
    return this.where(({exists}) => exists(relationship, cb, opts)) as Query<
      TTable,
      TSchema,
      TReturn
    >;
  }

  related(
    relationship: string,
    cb?: (q: AnyQuery) => AnyQuery,
  ): Query<TTable, TSchema, any> {
    if (relationship.startsWith(SUBQ_PREFIX)) {
      throw new Error(
        `Relationship names may not start with "${SUBQ_PREFIX}". That is a reserved prefix.`,
      );
    }
    cb = cb ?? (q => q);

    const related = this.#schema.relationships[this.#tableName][relationship];
    assert(related, 'Invalid relationship');
    if (isOneHop(related)) {
      const {destSchema, destField, sourceField, cardinality} = related[0];
      // Intern the base handed to the callback too. A deterministic callback
      // then walks the same interned chain and hands back the *same* sub-query,
      // which makes the comparison below a pointer compare.
      const q: AnyQuery = this.#deriveBounded(
        `relatedBase:${relationship}`,
        () =>
          this.#newQuery(
            destSchema,
            tableAST(destSchema, relationship),
            {
              relationships: {},
              singular: cardinality === 'one',
            },
            this.customQueryID,
            undefined,
          ),
      ) as AnyQuery;
      // Intentionally not setting to `one` as it is a perf degradation
      // and the user should not be making the mistake of setting cardinality to
      // `one` when it is actually not.
      // if (cardinality === 'one') {
      //   q = q.one();
      // }
      const subQuery = asQueryImpl(cb(q));
      assert(
        isCompoundKey(sourceField),
        'The source of a relationship must specify at last 1 field',
      );
      assert(
        isCompoundKey(destField),
        'The destination of a relationship must specify at last 1 field',
      );
      assert(
        sourceField.length === destField.length,
        'The source and destination of a relationship must have the same number of fields',
      );

      // Keyed by the sub-query's identity. Each query owns its AST object, so
      // this distinguishes even `q.one()` from `q.limit(1)`, whose ASTs are
      // structurally equal but whose formats differ. Comparing them as a delta
      // instead would put every sibling under one key and scan.
      return this.#derive(
        relatedKey(relationship),
        astID(subQuery.#ast),
        undefined,
        () =>
          this.#newQuery(
            this.#tableName,
            {
              ...this.#ast,
              related: insertRelated(this.#ast.related, {
                correlation: {
                  parentField: sourceField,
                  childField: destField,
                },
                subquery: subQuery.#ast,
                system: this.#system,
              }),
            },
            {
              ...this.format,
              relationships: {
                ...this.format.relationships,
                [relationship]: subQuery.format,
              },
            },
            this.customQueryID,
            this.#currentJunction,
          ),
      ) as AnyQuery;
    }

    if (isTwoHop(related)) {
      const [firstRelation, secondRelation] = related;
      const {destSchema} = secondRelation;
      const junctionSchema = firstRelation.destSchema;
      const sq = asQueryImpl(
        cb(
          this.#deriveBounded(`relatedBase:${relationship}`, () =>
            this.#newQuery(
              destSchema,
              tableAST(destSchema, relationship),
              {
                relationships: {},
                singular: secondRelation.cardinality === 'one',
              },
              this.customQueryID,
              relationship,
            ),
          ) as AnyQuery,
        ),
      );

      // Bound to consts so the `isCompoundKey` narrowing survives into the
      // closure below; TypeScript drops narrowing of property accesses there.
      const {sourceField: firstSource, destField: firstDest} = firstRelation;
      const {sourceField: secondSource, destField: secondDest} = secondRelation;
      assert(isCompoundKey(firstSource), 'Invalid relationship');
      assert(isCompoundKey(firstDest), 'Invalid relationship');
      assert(isCompoundKey(secondSource), 'Invalid relationship');
      assert(isCompoundKey(secondDest), 'Invalid relationship');

      return this.#derive(
        relatedKey(relationship),
        astID(sq.#ast),
        undefined,
        () =>
          this.#newQuery(
            this.#tableName,
            {
              ...this.#ast,
              related: insertRelated(this.#ast.related, {
                correlation: {
                  parentField: firstSource,
                  childField: firstDest,
                },
                hidden: true,
                subquery: {
                  ...tableAST(junctionSchema, relationship),
                  // A single subquery is sorted.
                  related: [
                    normalizedRelated({
                      correlation: {
                        parentField: secondSource,
                        childField: secondDest,
                      },
                      subquery: sq.#ast,
                      system: this.#system,
                    }),
                  ],
                },
                system: this.#system,
              }),
            },
            {
              ...this.format,
              relationships: {
                ...this.format.relationships,
                [relationship]: sq.format,
              },
            },
            this.customQueryID,
            this.#currentJunction,
          ),
      ) as AnyQuery;
    }

    throw new Error(`Invalid relationship ${relationship}`);
  }

  // The declared return is the *bottom* of the pinned dimension, which is
  // assignable to every `where` overload's declared return. The overloads
  // refine `TPinned` for callers only; a single non-generic implementation
  // signature cannot express that refinement.
  where(
    fieldOrExpressionFactory: string | ExpressionFactory<TTable, TSchema>,
    opOrValue?: SimpleOperator | GetFilterTypeAny | Parameter,
    value?: GetFilterTypeAny | Parameter,
  ): Query<TTable, TSchema, TReturn, any> {
    let cond: Condition;

    if (typeof fieldOrExpressionFactory === 'function') {
      cond = fieldOrExpressionFactory(this.expressionBuilder());
    } else {
      assert(arguments.length >= 2, 'Invalid condition. Too few arguments.');
      // Distinguish between 2-arg form (field, value) and 3-arg form (field, op, value)
      // using arguments.length to allow explicit undefined in 3-arg form.
      const twoArg = arguments.length === 2;
      const op = (twoArg ? '=' : opOrValue) as SimpleOperator;
      const raw = twoArg ? opOrValue : value;

      // Comparing a column to a primitive is by far the most common thing anyone
      // does with a query, and the transition it names is fully determined by
      // (column, op, value) -- so check for it before building anything. On a hit
      // this returns without allocating the `Condition` at all. Objects are
      // excluded, which covers both parameter references and `IN` arrays; `cmp`
      // maps a missing value to null, so this must too.
      if (raw === null || typeof raw !== 'object') {
        const hit = this.#transitions?.lookup(
          whereKey(fieldOrExpressionFactory, op),
          (raw ?? null) as TransitionValue,
          undefined,
        );
        if (hit !== undefined) {
          return hit as unknown as Query<TTable, TSchema, TReturn, any>;
        }
      }

      cond = twoArg
        ? cmp(fieldOrExpressionFactory, opOrValue)
        : cmp(fieldOrExpressionFactory, opOrValue, value);
    }

    // The delta is the condition as built here, *before* it is merged with the
    // existing where and normalized. Both steps are deterministic and the
    // existing where is fixed by this query, so an equal `cond` always produces
    // an equal result.
    //
    // A simple comparison against a column splits cleanly: the column and
    // operator come from a small fixed set and give a memoized, stable key,
    // while the compared value -- the part that varies per call -- is handed
    // through as the transition value and never concatenated into a string.
    let key: string;
    let tValue: TransitionValue;
    let delta: Delta = cond;
    if (cond.type === 'simple' && cond.left.type === 'column') {
      key = whereKey(cond.left.name, cond.op);
      const right = cond.right;
      tValue =
        right.type === 'literal' && isPrimitive(right.value)
          ? right.value
          : rightTag(right);
      delta = undefined;
    } else {
      // A sub-query an `exists` correlates to is itself interned, so its
      // identity settles that part of the tree without walking it.
      const exact = condKey(cond);
      if (exact === undefined) {
        key = treeKey(cond);
      } else {
        key = 'where:tree';
        tValue = exact;
        delta = undefined;
      }
    }

    return this.#derive(key, tValue, delta, () => {
      const existingWhere = this.#ast.where;
      const merged = existingWhere ? and(existingWhere, cond) : cond;
      return this.#newQuery(
        this.#tableName,
        {...this.#ast, where: normalizeCondition(simplifyCondition(merged))},
        this.format,
        this.customQueryID,
        this.#currentJunction,
      );
    }) as unknown as Query<TTable, TSchema, TReturn, any>;
  }

  start(
    row: Partial<Record<string, ReadonlyJSONValue | undefined>>,
    opts?: {inclusive: boolean},
  ): Query<TTable, TSchema, TReturn> {
    return this.#derive(
      opts?.inclusive ? 'start:inclusive' : 'start:exclusive',
      undefined,
      row,
      () =>
        this.#newQuery(
          this.#tableName,
          {
            ...this.#ast,
            start: {
              row,
              exclusive: !opts?.inclusive,
            },
          },
          this.format,
          this.customQueryID,
          this.#currentJunction,
        ),
    );
  }

  limit(limit: number): Query<TTable, TSchema, TReturn> {
    if (limit < 0) {
      throw new Error('Limit must be non-negative');
    }
    if ((limit | 0) !== limit) {
      throw new Error('Limit must be an integer');
    }
    if (this.#currentJunction) {
      throw new NotImplementedError(
        'Limit is not supported in junction relationships yet. Junction relationship being limited: ' +
          this.#currentJunction,
      );
    }

    return this.#derive('limit', limit, undefined, () =>
      this.#newQuery(
        this.#tableName,
        {...this.#ast, limit},
        this.format,
        this.customQueryID,
        this.#currentJunction,
      ),
    );
  }

  orderBy<TSelector extends keyof TSchema['tables'][TTable]['columns']>(
    field: TSelector,
    direction: 'asc' | 'desc',
  ): Query<TTable, TSchema, TReturn> {
    if (this.#currentJunction) {
      throw new NotImplementedError(
        'Order by is not supported in junction relationships yet. Junction relationship being ordered: ' +
          this.#currentJunction,
      );
    }
    return this.#derive(
      direction === 'asc' ? 'orderBy:asc' : 'orderBy:desc',
      field as string,
      undefined,
      () =>
        this.#newQuery(
          this.#tableName,
          {
            ...this.#ast,
            orderBy: [
              ...(this.#ast.orderBy ?? []),
              [field as string, direction],
            ],
          },
          this.format,
          this.customQueryID,
          this.#currentJunction,
        ),
    );
  }

  #exists(
    relationship: string,
    cb: ((query: AnyQuery) => AnyQuery) | undefined,
    options?: ExistsOptions,
  ): Condition {
    cb = cb ?? (q => q);
    const flip = options?.flip;
    const scalar = options?.scalar;
    const related = this.#schema.relationships[this.#tableName][relationship];
    assert(related, 'Invalid relationship');

    if (isOneHop(related)) {
      const {destSchema: destTableName, sourceField, destField} = related[0];
      assert(isCompoundKey(sourceField), 'Invalid relationship');
      assert(isCompoundKey(destField), 'Invalid relationship');

      const subQuery = asQueryImpl(
        cb(
          this.#deriveBounded(`existsBase:${relationship}`, () =>
            this.#newQuery(
              destTableName,
              tableAST(destTableName, `${SUBQ_PREFIX}${relationship}`),
              defaultFormat,
              this.customQueryID,
              undefined,
            ),
          ) as AnyQuery,
        ),
      );
      // Give the sub-query's AST an id so the enclosing `where` can key on it
      // exactly rather than comparing the sub-tree.
      astID(subQuery.#ast);
      // Unlike the entries of `related`, the correlated subqueries of a
      // condition keep the order their fields are written in: normalization
      // does not reorder them either.
      return {
        type: 'correlatedSubquery',
        related: {
          system: this.#system,
          correlation: {
            parentField: sourceField,
            childField: destField,
          },
          subquery: subQuery.#ast,
        },
        op: 'EXISTS',
        ...(flip !== undefined ? {flip} : {}),
        ...(scalar !== undefined ? {scalar} : {}),
      };
    }

    if (isTwoHop(related)) {
      const [firstRelation, secondRelation] = related;
      assert(isCompoundKey(firstRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(firstRelation.destField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.sourceField), 'Invalid relationship');
      assert(isCompoundKey(secondRelation.destField), 'Invalid relationship');
      const {destSchema} = secondRelation;
      const junctionSchema = firstRelation.destSchema;
      const queryToDest = cb(
        this.#deriveBounded(`existsJunctionBase:${relationship}`, () =>
          this.#newQuery(
            destSchema,
            tableAST(destSchema, `${SUBQ_PREFIX}zhidden_${relationship}`),
            defaultFormat,
            this.customQueryID,
            relationship,
          ),
        ) as AnyQuery,
      );

      return {
        type: 'correlatedSubquery',
        related: {
          system: this.#system,
          correlation: {
            parentField: firstRelation.sourceField,
            childField: firstRelation.destField,
          },
          subquery: {
            ...tableAST(junctionSchema, `${SUBQ_PREFIX}${relationship}`),
            // A single condition needs no flattening or sorting, but the node
            // itself still has to be rebuilt into the canonical field order.
            where: normalizeCondition({
              type: 'correlatedSubquery',
              related: {
                system: this.#system,
                correlation: {
                  parentField: secondRelation.sourceField,
                  childField: secondRelation.destField,
                },
                subquery: asQueryImpl(queryToDest).#ast,
              },
              op: 'EXISTS',
              ...(flip !== undefined ? {flip} : {}),
              ...(scalar !== undefined ? {scalar} : {}),
            }),
          },
        },
        op: 'EXISTS',
        ...(flip !== undefined ? {flip} : {}),
      };
    }

    throw new Error(`Invalid relationship ${relationship}`);
  }

  get ast(): NormalizedAST {
    return this.#ast;
  }

  expressionBuilder(): ExpressionBuilder<TTable, TSchema> {
    // Built on demand rather than held as an eagerly initialized field: a field
    // would cost a closure on every query constructed, where this costs one only
    // for queries that actually use an expression factory.
    return (this.#expressionBuilder ??= new ExpressionBuilder<TTable, TSchema>(
      this.#exists.bind(this) as ConstructorParameters<
        typeof ExpressionBuilder<TTable, TSchema>
      >[0],
    ));
  }
}

export function asQueryImpl<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
>(q: Query<TTable, TSchema, TReturn>): QueryImpl<TTable, TSchema, TReturn> {
  assert(q instanceof QueryImpl, 'Expected QueryImpl instance');
  return q;
}

/**
 * A stable id per query AST.
 *
 * Every `QueryImpl` allocates its own AST object, so these are one-to-one with
 * queries, and interning makes the object identity stable across rebuilds. That
 * lets a `related` or `exists` transition key on *which* sub-query it points at
 * rather than comparing the sub-tree.
 *
 * Assigned only where it is needed. A `WeakMap.set` costs on the order of a
 * hundred nanoseconds, so doing this for every query rather than for the few
 * that are sub-queries was one of the largest single costs of building a query
 * from cold. ASTs that never pass through here -- including the junction AST a
 * two-hop `exists` builds inline -- are absent, and fall back to a delta.
 */
const astIDs = new WeakMap<AST, number>();
let nextASTID = 0;

function astID(ast: AST): number {
  let id = astIDs.get(ast);
  if (id === undefined) {
    id = ++nextASTID;
    astIDs.set(ast, id);
  }
  return id;
}

/**
 * Memoized transition keys.
 *
 * A transition key must be a string V8 has already hashed, or the `Map` lookup
 * has to hash it from scratch -- measured at ~148ns against ~10ns, which dwarfs
 * everything else a lookup does. Both tables are keyed by schema-derived names,
 * so they are bounded by the schema.
 */
const whereKeys = new Map<string, Map<string, string>>();

function whereKey(column: string, op: string): string {
  let byOp = whereKeys.get(column);
  if (byOp === undefined) {
    byOp = new Map();
    whereKeys.set(column, byOp);
  }
  let key = byOp.get(op);
  if (key === undefined) {
    key = `where:${column}:${op}`;
    byOp.set(op, key);
  }
  return key;
}

const relatedKeys = new Map<string, string>();

function relatedKey(relationship: string): string {
  let key = relatedKeys.get(relationship);
  if (key === undefined) {
    key = `related:${relationship}`;
    relatedKeys.set(relationship, key);
  }
  return key;
}

function isPrimitive(v: LiteralValue): v is string | number | boolean | null {
  return v === null || typeof v !== 'object';
}

/**
 * Folds a JSON value into a self-delimiting token: a string is
 * length-prefixed, a number is terminated, an array or object is
 * count-prefixed and encodes its members recursively, and the rest are fixed
 * width. Tokens can therefore be concatenated into a key with no way for one
 * to run into the next, and the leading tag keeps `'1'` and `1` apart.
 *
 * Not `JSON.stringify`: it writes `Infinity`, `NaN` and `null` all as `null`,
 * and this token is trusted as exact, so a nested value that serialized like
 * another's would hand back a query with the wrong value in its AST. Numbers
 * are written the way `String` writes them, which keeps those apart. An
 * `undefined` property is skipped, as `deepEqual` would skip it; property
 * order is part of the encoding, so an object spelled in another order
 * interns separately.
 */
function valueTag(v: ReadonlyJSONValue): string {
  switch (typeof v) {
    case 'string':
      return `:s${enc(v)}`;
    case 'number':
      return `:n${v};`;
    case 'boolean':
      return v ? ':t' : ':f';
    default: {
      if (v === null) {
        return ':z';
      }
      if (Array.isArray(v)) {
        let out = `:a${v.length}:`;
        for (const e of v) {
          out += valueTag(e);
        }
        return out;
      }
      // `Array.isArray` does not narrow a readonly array out of the union.
      const o = v as ReadonlyJSONObject;
      let body = '';
      let n = 0;
      for (const key in o) {
        const e = o[key];
        if (e !== undefined && Object.hasOwn(o, key)) {
          body += enc(key) + valueTag(e);
          n++;
        }
      }
      return `:o${n}:${body}`;
    }
  }
}

/**
 * Folds the right-hand side of a simple condition into a transition value, so
 * that the transition is exact and the lookup needs no comparison at all.
 *
 * Arrays (`IN`) and parameter references are encoded in full: that is one
 * pass over a small value, where leaving them in the delta would instead cost
 * a `deepEqual` over the whole condition for every sibling a lookup walks
 * past.
 */
function rightTag(right: SimpleCondition['right']): string {
  if (right.type !== 'literal') {
    return `:p${valueTag(right as unknown as ReadonlyJSONValue)}`;
  }
  return valueTag(right.value);
}

/**
 * Length-prefixes a free-form string so it can be concatenated into a key
 * without a separator being ambiguous with the content.
 */
function enc(s: string): string {
  return `${s.length}:${s}`;
}

/** Encodes an optional boolean without collapsing `undefined` into `false`. */
function tri(b: boolean | undefined): string {
  return b === undefined ? '-' : b ? 't' : 'f';
}

/**
 * An exact key for a whole condition tree, or `undefined` for a shape we cannot
 * encode (which falls back to {@linkcode treeKey} plus a delta).
 *
 * Everything variable-length is length-prefixed, so distinct trees cannot
 * produce the same string and the key needs no `deepEqual` to back it up. That
 * matters because the alternative -- one coarse bucket per shape -- turns N
 * sibling conditions off the same parent into an N-deep scan of tree
 * comparisons, which measured far slower than not interning at all.
 */
function condKey(cond: Condition): string | undefined {
  switch (cond.type) {
    case 'simple': {
      if (cond.left.type !== 'column') {
        return undefined;
      }
      return `S${enc(cond.left.name)}${enc(cond.op)}${rightTag(cond.right)}`;
    }
    case 'correlatedSubquery': {
      const id = astIDs.get(cond.related.subquery);
      return id === undefined
        ? undefined
        : `E${id}:${enc(cond.op)}${tri(cond.flip)}${tri(cond.scalar)}`;
    }
    case 'and':
    case 'or': {
      let out = `${cond.type === 'and' ? 'A' : 'O'}${cond.conditions.length}:`;
      for (const c of cond.conditions) {
        const k = condKey(c);
        if (k === undefined) {
          return undefined;
        }
        out += k;
      }
      return out;
    }
    default:
      return undefined;
  }
}

/**
 * A coarse bucket key for the conditions {@linkcode condKey} cannot encode --
 * a two-hop `exists`, whose junction AST is built inline and so has no identity
 * of its own. Reads only the top of the tree, so it is cheap; the delta tells
 * apart what shares it.
 */
function treeKey(cond: Condition): string {
  switch (cond.type) {
    case 'correlatedSubquery':
      return `where:e:${cond.related.subquery.alias}:${cond.op}`;
    case 'and':
    case 'or':
      return `where:${cond.type}:${cond.conditions.length}`;
    default:
      return 'where:*';
  }
}

function throwQueryNotRunnable(): never {
  throw new Error('Query is not runnable');
}

function isCompoundKey(field: readonly string[]): field is CompoundKey {
  return Array.isArray(field) && field.length >= 1;
}

function isOneHop<T>(r: readonly T[]): r is readonly [T] {
  return r.length === 1;
}

function isTwoHop<T>(r: readonly T[]): r is readonly [T, T] {
  return r.length === 2;
}
