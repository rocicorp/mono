import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import {assignProperty} from '../../../shared/src/objects.ts';
import {
  AGGREGATE_TABLE_PREFIX,
  AGGREGATE_TABLE_SEPARATOR,
} from '../../../zero-protocol/src/aggregate-table.ts';
import type {
  AggregateFunction,
  AST,
  ColumnReference,
  CompoundKey,
  Condition,
  Conjunction,
  CorrelatedSubquery,
  CorrelatedSubqueryCondition,
  Disjunction,
  LiteralValue,
  Parameter,
  SimpleCondition,
  ValuePosition,
} from '../../../zero-protocol/src/ast.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {PrimaryKey} from '../../../zero-protocol/src/primary-key.ts';
import type {SchemaValue} from '../../../zero-types/src/schema-value.ts';
import {
  AGGREGATE_KEY_COLUMN,
  Aggregate,
  aggregateSourceSchema,
  assertNotPayloadColumn,
  isInvertible,
} from '../ivm/aggregate.ts';
import {Cap} from '../ivm/cap.ts';
import {Exists} from '../ivm/exists.ts';
import {FanIn} from '../ivm/fan-in.ts';
import {FanOut} from '../ivm/fan-out.ts';
import {
  buildFilterPipeline,
  type FilterInput,
} from '../ivm/filter-operators.ts';
import {Filter} from '../ivm/filter.ts';
import {FlippedJoin} from '../ivm/flipped-join.ts';
import {JoinIndexTap} from '../ivm/join-index-tap.ts';
import {JoinIndex} from '../ivm/join-index.ts';
import {Join, type ExistsJoin} from '../ivm/join.ts';
import {LiftField, LIFTED_FIELD_COLUMN} from '../ivm/lift-field.ts';
import type {Input, InputBase, Storage} from '../ivm/operator.ts';
import {Skip} from '../ivm/skip.ts';
import type {Source, SourceInput} from '../ivm/source.ts';
import {TakeGate, type TakeBoundProvider} from '../ivm/take-gate.ts';
import {Take} from '../ivm/take.ts';
import {UnionFanIn} from '../ivm/union-fan-in.ts';
import {UnionFanOut} from '../ivm/union-fan-out.ts';
import {planQuery, type PlanWarningSink} from '../planner/planner-builder.ts';
import type {ConnectionCostModel} from '../planner/planner-connection.ts';
import type {PlanDebugger} from '../planner/planner-debug.ts';
import {completeOrdering} from '../query/complete-ordering.ts';
import {pushDownCorrelatedPredicates} from './correlated-predicate-pushdown.ts';
import type {DebugDelegate} from './debug-delegate.ts';
import {
  createPredicate,
  transformFilters,
  type NoSubqueryCondition,
} from './filter.ts';

export type StaticQueryParameters = {
  authData: Record<string, JSONValue>;
  preMutationRow?: Row | undefined;
};

/**
 * Interface required of caller to buildPipeline. Connects to constructed
 * pipeline to delegate environment to provide sources and storage.
 */
export interface BuilderDelegate {
  readonly applyFiltersAnyway?: boolean | undefined;
  debug?: DebugDelegate | undefined;

  /**
   * When true, allows NOT EXISTS conditions in queries.
   * Defaults to false.
   *
   * We only set this to true on the server.
   * The client-side query engine cannot support NOT EXISTS because:
   * 1. Zero only syncs a subset of data to the client
   * 2. On the client, we can't distinguish between a row not existing vs.
   *    a row not being synced to the client
   * 3. NOT EXISTS requires complete knowledge of what doesn't exist
   */
  readonly enableNotExists?: boolean | undefined;

  /**
   * When true, does not copy a parent's conditions on correlation columns
   * into its subqueries (see {@link pushDownCorrelatedPredicates}).
   * Defaults to false.
   *
   * Only zero-cache sets this, as a kill switch.
   */
  readonly disableCorrelatedPredicatePushdown?: boolean | undefined;

  /**
   * When true, copies a parent's conditions into its subqueries before
   * planning instead of after, and tells the planner which conditions it
   * copied. The planner then sees when a copied condition makes a flipped
   * join cheap. Has no effect when `disableCorrelatedPredicatePushdown` is
   * true. Defaults to false.
   *
   * Only zero-cache sets this. This changes plans, and the pushdown alone
   * does not.
   */
  readonly enablePlannerAwarePushdown?: boolean | undefined;

  /**
   * When true, an `aggregate` relationship is read from a synthetic,
   * pre-computed aggregate source (see {@link aggregateTableName}) via a
   * plain Join, instead of being computed by an {@link Aggregate} operator
   * over the child rows.
   *
   * This is the *synced client* mode: the server computes the aggregate (the
   * Aggregate operator consumes the child rows so they never sync) and streams
   * only the per-parent result rows; the client reads them and never holds the
   * children. Defaults to false (compute locally), which is correct for the
   * server and for single-process client materialization.
   */
  readonly aggregatesFromSource?: boolean | undefined;

  /**
   * Called once for each source needed by the AST.
   * Might be called multiple times with same tableName. It is OK to return
   * same storage instance in that case.
   */
  getSource(tableName: string): Source | undefined;

  /**
   * Get-or-create the synthetic source for a *relationship* aggregate read in
   * `aggregatesFromSource` mode. Unlike a top-level aggregate (whose shape is
   * derivable from the table name), a relationship aggregate's source needs the
   * correlation-key columns + types, which only the builder knows here — so it
   * passes them in. Optional: only the synced client implements it; the server
   * (compute mode) never takes the `aggregatesFromSource` path, and tests may
   * instead provide the source directly via {@link getSource}.
   */
  getAggregateSource?(
    name: string,
    columns: Record<string, SchemaValue>,
    primaryKey: PrimaryKey,
    /**
     * When present, the client may optimistically update this aggregate when a
     * child row in `table` is locally mutated (only supplied for the invertible
     * cases with a per-row-evaluable `where` — see the call site). Omitted ⇒
     * server-authoritative only.
     */
    optimisticDelta?: AggregateOptimisticDelta,
  ): Source;

  /**
   * The names under which a synced aggregate row carries the key `columns` of
   * `table`, in the same order. The server computes aggregates over the AST it
   * was sent, which the client maps to server names, so the rows it streams
   * key by server column names; the synthetic source has to match them. Only
   * the synced client implements it; absent, the names are used unchanged.
   */
  aggregateKeyColumns?(table: string, columns: CompoundKey): CompoundKey;

  /**
   * Called once for each operator that requires storage. Should return a new
   * unique storage object for each call.
   */
  createStorage(name: string): Storage;

  decorateInput(input: Input, name: string): Input;

  addEdge(source: InputBase, dest: InputBase): void;

  decorateFilterInput(input: FilterInput, name: string): FilterInput;

  decorateSourceInput(input: SourceInput, queryID: string): Input;

  /**
   * Receives warnings about the plan the planner chose for the query, when
   * there is a cost model to plan with. Only zero-cache sets this.
   */
  readonly planWarnings?: PlanWarningSink | undefined;

  /**
   * The AST is mapped on-the-wire between client and server names.
   *
   * There is no "wire" for zqlite tests so this function is provided
   * to allow tests to remap the AST.
   */
  mapAst?: ((ast: AST) => AST) | undefined;
}

/**
 * Builds a pipeline from an AST. Caller must provide a delegate to create source
 * and storage interfaces as necessary.
 *
 * Usage:
 *
 * ```ts
 * class MySink implements Output {
 *   readonly #input: Input;
 *
 *   constructor(input: Input) {
 *     this.#input = input;
 *     input.setOutput(this);
 *   }
 *
 *   push(change: Change, _: Operator) {
 *     console.log(change);
 *   }
 * }
 *
 * const input = buildPipeline(ast, myDelegate, hash(ast));
 * const sink = new MySink(input);
 * ```
 */
export function buildPipeline(
  ast: AST,
  delegate: BuilderDelegate,
  queryID: string,
  costModel?: ConnectionCostModel,
  lc?: LogContext,
  planDebugger?: PlanDebugger,
): Input {
  ast = delegate.mapAst ? delegate.mapAst(ast) : ast;

  // Synced client reading a top-level (ungrouped) aggregate: the underlying
  // table is not synced and has no source here, and ordering/planning don't
  // apply to a scalar result. Read the precomputed row directly from the
  // synthetic source (see buildPipelineInternal's short-circuit).
  if (ast.aggregate && delegate.aggregatesFromSource) {
    return buildPipelineInternal(ast, delegate, queryID, '');
  }

  ast = completeOrdering(
    ast,
    tableName => must(delegate.getSource(tableName)).tableSchema.primaryKey,
  );

  const columnsOf = (tableName: string) =>
    must(delegate.getSource(tableName)).tableSchema.columns;
  const {planWarnings} = delegate;
  if (delegate.disableCorrelatedPredicatePushdown) {
    if (costModel) {
      ast = planQuery(
        ast,
        costModel,
        planDebugger,
        lc,
        undefined,
        planWarnings,
      );
    }
  } else if (delegate.enablePlannerAwarePushdown) {
    const pushed = new Set<SimpleCondition>();
    ast = pushDownCorrelatedPredicates(ast, columnsOf, pushed);
    if (costModel) {
      ast = planQuery(ast, costModel, planDebugger, lc, pushed, planWarnings);
    }
  } else {
    if (costModel) {
      ast = planQuery(
        ast,
        costModel,
        planDebugger,
        lc,
        undefined,
        planWarnings,
      );
    }
    // After planning, so that the planner does not read the pushed conditions
    // as selective filters.
    ast = pushDownCorrelatedPredicates(ast, columnsOf);
  }
  return buildPipelineInternal(ast, delegate, queryID, '');
}

export function bindStaticParameters(
  ast: AST,
  staticQueryParameters: StaticQueryParameters | undefined,
) {
  const visit = (node: AST): AST => ({
    ...node,
    where: node.where ? bindCondition(node.where) : undefined,
    related: node.related?.map(sq => ({
      ...sq,
      subquery: visit(sq.subquery),
    })),
  });

  function bindCondition(condition: Condition): Condition {
    if (condition.type === 'simple') {
      return {
        ...condition,
        left: bindValue(condition.left),
        right: bindValue(condition.right) as Exclude<
          ValuePosition,
          ColumnReference
        >,
      };
    }
    if (condition.type === 'correlatedSubquery') {
      return {
        ...condition,
        related: {
          ...condition.related,
          subquery: visit(condition.related.subquery),
        },
      };
    }

    return {
      ...condition,
      conditions: condition.conditions.map(bindCondition),
    };
  }

  const bindValue = (value: ValuePosition): ValuePosition => {
    if (isParameter(value)) {
      const anchor = must(
        staticQueryParameters,
        'Static query params do not exist',
      )[value.anchor];
      const resolvedValue = resolveField(anchor, value.field);
      return {
        type: 'literal',
        value: resolvedValue as LiteralValue,
      };
    }
    return value;
  };

  return visit(ast);
}

function resolveField(
  anchor: Record<string, JSONValue> | Row | undefined,
  field: string | string[],
): unknown {
  if (anchor === undefined) {
    return null;
  }

  if (Array.isArray(field)) {
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    return field.reduce((acc, f) => (acc as any)?.[f], anchor) ?? null;
  }

  return anchor[field] ?? null;
}

function isParameter(value: ValuePosition): value is Parameter {
  return value.type === 'static';
}

const EXISTS_LIMIT = 3;
const PERMISSIONS_EXISTS_LIMIT = 1;

/**
 * Checks if a condition tree contains any NOT EXISTS operations.
 * Recursively checks AND/OR branches but does not recurse into nested subqueries
 * (those are checked when buildPipelineInternal processes them).
 */
function assertNoNotExists(condition: Condition): void {
  switch (condition.type) {
    case 'simple':
      return;

    case 'correlatedSubquery':
      if (condition.op === 'NOT EXISTS') {
        throw new Error(
          'not(exists()) is not supported on the client - see https://bugs.rocicorp.dev/issue/3438',
        );
      }
      return;

    case 'and':
    case 'or':
      for (const c of condition.conditions) {
        assertNoNotExists(c);
      }
      return;
    default:
      unreachable(condition);
  }
}

function buildPipelineInternal(
  ast: AST,
  delegate: BuilderDelegate,
  queryID: string,
  name: string,
  partitionKey?: CompoundKey,
  isNonFlippedExistsChild?: boolean,
): Input {
  // Synced client reading a top-level (ungrouped) aggregate: the precomputed
  // single row is read from the synthetic source `aggregate:<queryID>`; the
  // underlying table is never synced, so its pipeline is not built at all.
  if (ast.aggregate && delegate.aggregatesFromSource) {
    const aggTable = topLevelAggregateTableName(queryID);
    const aggSource = delegate.getSource(aggTable);
    if (!aggSource) {
      throw new Error(`Aggregate source not found: ${aggTable}`);
    }
    const conn = aggSource.connect([[AGGREGATE_KEY_COLUMN, 'asc']]);
    return delegate.decorateInput(
      delegate.decorateSourceInput(conn, queryID),
      `${name}:aggregateSource`,
    );
  }

  const source = delegate.getSource(ast.table);
  if (!source) {
    throw new Error(`Source not found: ${ast.table}`);
  }

  ast = uniquifyCorrelatedSubqueryConditionAliases(ast);

  if (!delegate.enableNotExists && ast.where) {
    assertNoNotExists(ast.where);
  }

  const csqConditions = gatherCorrelatedSubqueryQueryConditions(ast.where);
  const splitEditKeys: Set<string> = partitionKey
    ? new Set(partitionKey)
    : new Set();
  const aliases = new Set<string>();
  for (const csq of csqConditions) {
    aliases.add(csq.related.subquery.alias || '');
    for (const key of csq.related.correlation.parentField) {
      splitEditKeys.add(key);
    }
  }
  if (ast.related) {
    for (const csq of ast.related) {
      for (const key of csq.correlation.parentField) {
        splitEditKeys.add(key);
      }
    }
  }
  if (isNonFlippedExistsChild) {
    assert(ast.start === undefined, 'EXISTS subqueries must not have start');
    assert(
      ast.related === undefined,
      'EXISTS subqueries must not have related',
    );
  }

  // The Cap optimization needs the source connect to be unordered, but
  // applyFilterWithFlips builds a UnionFanIn over the source whenever
  // ast.where contains a flipped subquery, and UnionFanIn requires a
  // sort on its inputs. In that case, fall back to the ordered + Take
  // path for this EXISTS child.
  const useCap =
    isNonFlippedExistsChild &&
    !(ast.where && conditionIncludesFlippedSubqueryAtAnyLevel(ast.where));

  const conn = source.connect(
    // exists pipelines are unordered — orderBy is ignored here.
    // Non-exists pipelines always have orderBy completed with PKs.
    useCap ? undefined : must(ast.orderBy),
    ast.where,
    splitEditKeys,
    delegate.debug,
  );

  let end: Input = delegate.decorateSourceInput(conn, queryID);
  end = delegate.decorateInput(end, `${name}:source(${ast.table})`);
  const {fullyAppliedFilters} = conn;

  if (ast.start) {
    const skip = new Skip(end, ast.start);
    delegate.addEdge(end, skip);
    end = delegate.decorateInput(skip, `${name}:skip)`);
  }

  let takeGate: TakeGate | undefined;
  if (ast.limit !== undefined && !useCap) {
    const takeGateName = `${name}:take-gate`;
    takeGate = new TakeGate(end);
    delegate.addEdge(end, takeGate);
    end = delegate.decorateInput(takeGate, takeGateName);
  }

  // The parent indexes of the EXISTS joins below. They hold the rows this
  // pipeline emits, which the JoinIndexTap after the limit maintains.
  const existsParentIndexes: JoinIndex[] = [];
  for (const csqCondition of csqConditions) {
    // flipped EXISTS are handled in applyWhere
    if (!csqCondition.flip) {
      const parentIndex = new JoinIndex(
        csqCondition.related.correlation.parentField,
        end.getSchema().primaryKey,
      );
      existsParentIndexes.push(parentIndex);
      end = applyCorrelatedSubQuery(
        {
          ...csqCondition.related,
          subquery: {
            ...csqCondition.related.subquery,
            limit:
              csqCondition.related.system === 'permissions'
                ? PERMISSIONS_EXISTS_LIMIT
                : EXISTS_LIMIT,
          },
        },
        delegate,
        queryID,
        end,
        name,
        true,
        undefined,
        takeGate,
        {op: csqCondition.op, parentIndex},
      );
    }
  }

  if (ast.where && (!fullyAppliedFilters || delegate.applyFiltersAnyway)) {
    end = applyWhere(
      end,
      ast.where,
      delegate,
      name,
      partitionKey,
      takeGate,
      existsParentIndexes,
    );
  }

  if (ast.limit !== undefined) {
    // We end `exists` pipelines with `cap`
    // The reason is that `cap` does not care about the order of the pipeline.
    // This allows SQLite to chose the order and never end up creating temp b-trees.
    // The problem with SQLite creating a temp b-tree is it will incur a scan of the entire
    // result set where exists only needs the first row.
    if (useCap) {
      const capName = `${name}:cap`;
      const cap = new Cap(
        end,
        delegate.createStorage(capName),
        ast.limit,
        partitionKey,
      );
      delegate.addEdge(end, cap);
      end = delegate.decorateInput(cap, capName);
    } else {
      const takeName = `${name}:take`;
      const take = new Take(
        end,
        delegate.createStorage(takeName),
        ast.limit,
        partitionKey,
      );
      delegate.addEdge(end, take);
      end = delegate.decorateInput(take, takeName);
      takeGate?.setBoundProvider(take);
      if (takeGate) {
        take.setTakeGate(takeGate);
      }
    }
  }

  if (existsParentIndexes.length > 0) {
    const tapName = `${name}:join-index`;
    const tap = new JoinIndexTap(end, existsParentIndexes);
    delegate.addEdge(end, tap);
    end = delegate.decorateInput(tap, tapName);
  }

  if (ast.aggregate) {
    // Top-level (ungrouped) aggregate, compute mode (server / local): the
    // Aggregate operator reduces the rows that survive `where`, `start` and
    // `limit` to one synthetic row emitted to `aggregate:<queryID>`, so
    // `limit(10).count()` counts at most 10 rows, as in SQL. `related` does not
    // apply to a scalar result. (The synced client takes the
    // aggregatesFromSource short-circuit at the top of this function instead.)
    //
    // When the source applies every filter and nothing else sits between it
    // and the Aggregate (no `start`, `limit` or EXISTS join), the source's
    // rows are exactly the rows to reduce, so a `count` can ask it for the
    // number rather than reading them all.
    const sourceAppliesAll =
      ast.start === undefined &&
      ast.limit === undefined &&
      csqConditions.length === 0 &&
      (!ast.where || (fullyAppliedFilters && !delegate.applyFiltersAnyway));
    const aggName = `${name}:aggregate`;
    const aggregate = new Aggregate(
      end,
      delegate.createStorage(aggName),
      [], // ungrouped — one global group
      ast.aggregate.fn,
      ast.aggregate.field,
      topLevelAggregateTableName(queryID),
      sourceAppliesAll ? conn : undefined,
    );
    delegate.addEdge(end, aggregate);
    return delegate.decorateInput(aggregate, aggName);
  }

  if (ast.related) {
    // Dedupe by alias - last one wins (LWW), like limit(5).limit(10)
    const byAlias = new Map<string, CorrelatedSubquery>();
    for (const csq of ast.related) {
      byAlias.set(csq.subquery.alias ?? '', csq);
    }
    for (const csq of byAlias.values()) {
      end = applyCorrelatedSubQuery(
        csq,
        delegate,
        queryID,
        end,
        name,
        false,
        partitionKey,
      );
    }
  }

  return end;
}

function applyWhere(
  input: Input,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
  parentPartitionKey?: CompoundKey,
  boundProvider?: TakeBoundProvider,
  existsParentIndexes?: JoinIndex[],
): Input {
  if (!conditionIncludesFlippedSubqueryAtAnyLevel(condition)) {
    return buildFilterPipeline(
      input,
      delegate,
      filterInput => applyFilter(filterInput, condition, delegate, name),
      transformFilters(condition).filters,
    );
  }

  return applyFilterWithFlips(
    input,
    condition,
    delegate,
    name,
    parentPartitionKey,
    boundProvider,
    existsParentIndexes,
  );
}

function applyFilterWithFlips(
  input: Input,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
  parentPartitionKey?: CompoundKey,
  boundProvider?: TakeBoundProvider,
  existsParentIndexes?: JoinIndex[],
): Input {
  let end = input;
  assert(condition.type !== 'simple', 'Simple conditions cannot have flips');

  switch (condition.type) {
    case 'and': {
      const [withFlipped, withoutFlipped] = partitionBranches(
        condition.conditions,
        conditionIncludesFlippedSubqueryAtAnyLevel,
      );
      if (withoutFlipped.length > 0) {
        const andCondition: Conjunction = {
          type: 'and',
          conditions: withoutFlipped,
        };
        end = buildFilterPipeline(
          input,
          delegate,
          filterInput => applyAnd(filterInput, andCondition, delegate, name),
          transformFilters(andCondition).filters,
        );
      }
      assert(withFlipped.length > 0, 'Impossible to have no flips here');
      for (const cond of withFlipped) {
        end = applyFilterWithFlips(
          end,
          cond,
          delegate,
          name,
          parentPartitionKey,
          boundProvider,
          existsParentIndexes,
        );
      }
      break;
    }
    case 'or': {
      const [withFlipped, withoutFlipped] = partitionBranches(
        condition.conditions,
        conditionIncludesFlippedSubqueryAtAnyLevel,
      );
      assert(withFlipped.length > 0, 'Impossible to have no flips here');

      const ufo = new UnionFanOut(end);
      delegate.addEdge(end, ufo);
      end = delegate.decorateInput(ufo, `${name}:ufo`);

      const branches: Input[] = [];
      if (withoutFlipped.length > 0) {
        const orCondition: Disjunction = {
          type: 'or',
          conditions: withoutFlipped,
        };
        branches.push(
          buildFilterPipeline(
            end,
            delegate,
            filterInput => applyOr(filterInput, orCondition, delegate, name),
            transformFilters(orCondition).filters,
          ),
        );
      }

      for (const cond of withFlipped) {
        branches.push(
          applyFilterWithFlips(
            end,
            cond,
            delegate,
            name,
            parentPartitionKey,
            boundProvider,
            existsParentIndexes,
          ),
        );
      }

      const ufi = new UnionFanIn(ufo, branches);
      for (const branch of branches) {
        delegate.addEdge(branch, ufi);
      }
      end = delegate.decorateInput(ufi, `${name}:ufi`);

      break;
    }
    case 'correlatedSubquery': {
      const sq = condition.related;
      const child = buildPipelineInternal(
        sq.subquery,
        delegate,
        '',
        childPipelineName(name, sq),
        sq.correlation.childField,
        false,
      );
      let parentIndex: JoinIndex | undefined;
      if (existsParentIndexes) {
        parentIndex = new JoinIndex(
          sq.correlation.parentField,
          end.getSchema().primaryKey,
          parentPartitionKey,
        );
        existsParentIndexes.push(parentIndex);
      }
      const flippedJoinName = `${name}:flipped-join(${sq.subquery.alias})`;
      const flippedJoin = new FlippedJoin({
        parent: end,
        child,
        parentKey: sq.correlation.parentField,
        childKey: sq.correlation.childField,
        relationshipName: must(
          sq.subquery.alias,
          'Subquery must have an alias',
        ),
        hidden: sq.hidden ?? false,
        system: sq.system ?? 'client',
        parentPartitionKey,
        boundProvider,
        parentIndex,
      });
      delegate.addEdge(end, flippedJoin);
      delegate.addEdge(child, flippedJoin);
      end = delegate.decorateInput(flippedJoin, flippedJoinName);
      break;
    }
  }

  return end;
}

function applyFilter(
  input: FilterInput,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
): FilterInput {
  switch (condition.type) {
    case 'and':
      return applyAnd(input, condition, delegate, name);
    case 'or':
      return applyOr(input, condition, delegate, name);
    case 'correlatedSubquery':
      return applyCorrelatedSubqueryCondition(input, condition, delegate, name);
    case 'simple':
      return applySimpleCondition(input, delegate, condition);
  }
}

function applyAnd(
  input: FilterInput,
  condition: Conjunction,
  delegate: BuilderDelegate,
  name: string,
): FilterInput {
  for (const subCondition of condition.conditions) {
    input = applyFilter(input, subCondition, delegate, name);
  }
  return input;
}

function applyOr(
  input: FilterInput,
  condition: Disjunction,
  delegate: BuilderDelegate,
  name: string,
): FilterInput {
  const [subqueryConditions, otherConditions] =
    groupSubqueryConditions(condition);
  // if there are no subquery conditions, no fan-in / fan-out is needed
  if (subqueryConditions.length === 0) {
    const filter = new Filter(
      input,
      createPredicate({
        type: 'or',
        conditions: otherConditions,
      }),
    );
    delegate.addEdge(input, filter);
    return filter;
  }

  const fanOut = new FanOut(input);
  delegate.addEdge(input, fanOut);
  const branches = subqueryConditions.map(subCondition =>
    applyFilter(fanOut, subCondition, delegate, name),
  );
  if (otherConditions.length > 0) {
    const filter = new Filter(
      fanOut,
      createPredicate({
        type: 'or',
        conditions: otherConditions,
      }),
    );
    delegate.addEdge(fanOut, filter);
    branches.push(filter);
  }
  const ret = new FanIn(fanOut, branches);
  for (const branch of branches) {
    delegate.addEdge(branch, ret);
  }
  fanOut.setFanIn(ret);
  return ret;
}

export function groupSubqueryConditions(condition: Disjunction) {
  const partitioned: [
    subqueryConditions: Condition[],
    otherConditions: NoSubqueryCondition[],
  ] = [[], []];
  for (const subCondition of condition.conditions) {
    if (isNotAndDoesNotContainSubquery(subCondition)) {
      partitioned[1].push(subCondition);
    } else {
      partitioned[0].push(subCondition);
    }
  }
  return partitioned;
}

function isNotAndDoesNotContainSubquery(
  condition: Condition,
): condition is NoSubqueryCondition {
  if (condition.type === 'correlatedSubquery') {
    return false;
  }
  if (condition.type === 'simple') {
    return true;
  }
  return condition.conditions.every(isNotAndDoesNotContainSubquery);
}

function applySimpleCondition(
  input: FilterInput,
  delegate: BuilderDelegate,
  condition: SimpleCondition,
): FilterInput {
  const filter = new Filter(input, createPredicate(condition));
  delegate.decorateFilterInput(
    filter,
    `${valuePosName(condition.left)}:${condition.op}:${valuePosName(condition.right)}`,
  );
  delegate.addEdge(input, filter);
  return filter;
}

function valuePosName(left: ValuePosition) {
  switch (left.type) {
    case 'static':
      return left.field;
    case 'literal':
      return left.value;
    case 'column':
      return left.name;
  }
}

function applyCorrelatedSubQuery(
  sq: CorrelatedSubquery,
  delegate: BuilderDelegate,
  queryID: string,
  end: Input,
  name: string,
  fromCondition: boolean,
  parentPartitionKey?: CompoundKey,
  boundProvider?: TakeBoundProvider,
  exists?: ExistsJoin,
) {
  // TODO: we only omit the join if the CSQ if from a condition since
  // we want to create an empty array for `related` fields that are `limit(0)`
  if (sq.subquery.limit === 0 && fromCondition) {
    return end;
  }

  assert(sq.subquery.alias, 'Subquery must have an alias');

  // An `aggregate` relationship (count/sum/avg/min/max).
  //
  // The Join below is unchanged regardless of mode: the aggregate "child" emits
  // a single synthetic row per parent carrying the correlation key, so Join
  // routes by childField as usual.
  //
  // Two modes (see BuilderDelegate.aggregatesFromSource):
  //  - compute (server / single-process client): an Aggregate operator reduces
  //    the child rows. It consumes them, so the children never flow downstream
  //    (i.e. never sync). Its output rows are routed to a synthetic table.
  //  - source (synced client): the children are not present at all; read the
  //    pre-computed results from the synthetic aggregate source.
  let childInput: Input;
  // The columns the child input is keyed by for the join. For a synced
  // aggregate these are the names the server streams the rows with.
  let childKey = sq.correlation.childField;
  if (sq.aggregate) {
    const aggTable = aggregateTableName(queryID, sq, name);
    if (delegate.aggregatesFromSource) {
      childKey =
        delegate.aggregateKeyColumns?.(
          sq.subquery.table,
          sq.correlation.childField,
        ) ?? sq.correlation.childField;
      // The server builds its operator with these names and rejects a
      // collision there; check here too so the client fails the same way.
      for (const col of childKey) {
        assertNotPayloadColumn(col);
      }
      // Always go through getAggregateSource when the delegate has one, even
      // if the source already exists: on reload it is provisioned from the
      // persisted rows before the query materializes (see IVMSourceBranch
      // applyDiffs), and this call is what registers the optimistic-delta
      // metadata. The delegate hands back the existing source in that case.
      let source: Source | undefined;
      if (delegate.getAggregateSource) {
        // The synthetic source isn't a schema table; provision it from the
        // child's column types + the aggregate fn/field so the synced rows have
        // a correctly-shaped place to land (key = correlation child field).
        const childSource = delegate.getSource(sq.subquery.table);
        if (!childSource) {
          throw new Error(`Source not found: ${sq.subquery.table}`);
        }
        // For a junction (many-to-many) aggregate the key lives on the junction
        // table but the aggregated field lives on the destination one hop past
        // it — so merge the destination's columns in for the value-type lookup.
        let inputColumns = childSource.tableSchema.columns;
        const isJunction = !!(
          sq.subquery.related && sq.subquery.related.length
        );
        if (isJunction) {
          const destTable = must(sq.subquery.related)[0].subquery.table;
          const destSource = delegate.getSource(destTable);
          if (!destSource) {
            throw new Error(`Source not found: ${destTable}`);
          }
          inputColumns = {
            ...childSource.tableSchema.columns,
            ...destSource.tableSchema.columns,
          };
        }
        const {columns: clientColumns} = aggregateSourceSchema(
          inputColumns,
          sq.correlation.childField,
          sq.aggregate.fn,
          sq.aggregate.field,
        );
        const columns = renameColumns(
          clientColumns,
          sq.correlation.childField,
          childKey,
        );
        const {fn} = sq.aggregate;
        const invertible = isInvertible(fn);
        // A `where` is honored optimistically by compiling it to a per-row
        // predicate, but only when fully per-row evaluable. A correlated
        // subquery in the `where` can't be judged from a single child row, so
        // `transformFilters` flags it (`conditionsRemoved`) and the aggregate
        // stays server-authoritative rather than risk over-counting.
        let whereEvaluable = true;
        let wherePredicate: ((row: Row) => boolean) | undefined;
        if (!isJunction && invertible && sq.subquery.where !== undefined) {
          const {filters, conditionsRemoved} = transformFilters(
            sq.subquery.where,
          );
          if (conditionsRemoved || filters === undefined) {
            whereEvaluable = false;
          } else {
            wherePredicate = createPredicate(filters);
          }
        }
        source = delegate.getAggregateSource(
          aggTable,
          columns,
          childKey,
          // Optimistic deltas are only safe for invertible, direct
          // (non-junction) aggregates: `count` (±1), `sum`/`avg` (±field) per
          // child add/remove. `min`/`max` aren't invertible, and a junction's
          // field lives past the junction (an edge mutation doesn't carry it) —
          // both fall back to the server-authoritative value, as does a `where`
          // that isn't per-row evaluable (see above) and a bounded subquery
          // (`limit`/`start`), where whether a child counts depends on its
          // siblings.
          !isJunction &&
            invertible &&
            whereEvaluable &&
            sq.subquery.limit === undefined &&
            sq.subquery.start === undefined
            ? {
                table: sq.subquery.table,
                childField: sq.correlation.childField,
                keyField: childKey,
                fn,
                field: sq.aggregate.field,
                predicate: wherePredicate,
              }
            : undefined,
        );
      } else {
        source = delegate.getSource(aggTable);
      }
      if (!source) {
        throw new Error(`Aggregate source not found: ${aggTable}`);
      }
      const conn = source.connect(childKey.map(f => [f, 'asc'] as const));
      childInput = delegate.decorateSourceInput(conn, queryID);
      childInput = delegate.decorateInput(
        childInput,
        `${name}:aggregateSource(${sq.subquery.alias})`,
      );
    } else {
      const child = buildPipelineInternal(
        sq.subquery,
        delegate,
        queryID,
        childPipelineName(name, sq),
        sq.correlation.childField,
        fromCondition,
      );
      const aggName = `${name}:aggregate(${sq.subquery.alias})`;

      // Junction (many-to-many) aggregate: the subquery is the junction table
      // with a related() to the destination, and the aggregated field lives on
      // that destination. Lift the field onto the junction row so the Aggregate
      // (which reads a flat field) collapses it as a single hop. `count` never
      // takes this path — it's collapsed to a plain count over the junction.
      let aggInput = child;
      let aggField = sq.aggregate.field;
      if (sq.subquery.related && sq.subquery.related.length > 0) {
        const destAlias = must(sq.subquery.related[0].subquery.alias);
        const lift = new LiftField(child, destAlias, must(sq.aggregate.field));
        delegate.addEdge(child, lift);
        aggInput = lift;
        aggField = LIFTED_FIELD_COLUMN;
      }

      const aggregate = new Aggregate(
        aggInput,
        delegate.createStorage(aggName),
        sq.correlation.childField,
        sq.aggregate.fn,
        aggField,
        aggTable,
      );
      delegate.addEdge(aggInput, aggregate);
      childInput = delegate.decorateInput(aggregate, aggName);
    }
  } else {
    childInput = buildPipelineInternal(
      sq.subquery,
      delegate,
      queryID,
      childPipelineName(name, sq),
      sq.correlation.childField,
      fromCondition,
    );
  }

  const joinName = `${name}:join(${sq.subquery.alias})`;
  const join = new Join({
    parent: end,
    child: childInput,
    parentKey: sq.correlation.parentField,
    childKey,
    relationshipName: sq.subquery.alias,
    hidden: sq.hidden ?? false,
    system: sq.system ?? 'client',
    parentPartitionKey: fromCondition ? undefined : parentPartitionKey,
    boundProvider,
    exists,
  });
  delegate.addEdge(end, join);
  delegate.addEdge(childInput, join);
  return delegate.decorateInput(join, joinName);
}

function applyCorrelatedSubqueryCondition(
  input: FilterInput,
  condition: CorrelatedSubqueryCondition,
  delegate: BuilderDelegate,
  name: string,
): FilterInput {
  assert(
    condition.op === 'EXISTS' || condition.op === 'NOT EXISTS',
    'Expected EXISTS or NOT EXISTS operator',
  );
  if (condition.related.subquery.limit === 0) {
    if (condition.op === 'EXISTS') {
      const filter = new Filter(input, () => false);
      delegate.addEdge(input, filter);
      return filter;
    }
    const filter = new Filter(input, () => true);
    delegate.addEdge(input, filter);
    return filter;
  }
  const existsName = `${name}:exists(${condition.related.subquery.alias})`;
  const exists = new Exists(
    input,
    must(condition.related.subquery.alias),
    condition.related.correlation.parentField,
    condition.op,
  );
  delegate.addEdge(input, exists);
  return delegate.decorateFilterInput(exists, existsName);
}

function gatherCorrelatedSubqueryQueryConditions(
  condition: Condition | undefined,
) {
  const csqs: CorrelatedSubqueryCondition[] = [];
  const gather = (condition: Condition) => {
    if (condition.type === 'correlatedSubquery') {
      csqs.push(condition);
      return;
    }
    if (condition.type === 'and' || condition.type === 'or') {
      for (const c of condition.conditions) {
        gather(c);
      }
      return;
    }
  };
  if (condition) {
    gather(condition);
  }
  return csqs;
}

/** `columns` with each of `from` renamed to the name at its index in `to`. */
function renameColumns(
  columns: Record<string, SchemaValue>,
  from: CompoundKey,
  to: CompoundKey,
): Record<string, SchemaValue> {
  const renamed: Record<string, SchemaValue> = {};
  for (const [name, schema] of Object.entries(columns)) {
    const i = from.indexOf(name);
    assignProperty(renamed, i === -1 ? name : to[i], schema);
  }
  return renamed;
}

/**
 * What the synced client needs to optimistically update a relationship
 * aggregate when one of its child rows is mutated locally.
 */
export type AggregateOptimisticDelta = {
  /** The child table whose mutations move the aggregate. */
  readonly table: string;
  /** The correlation columns of a child row, as the client names them. */
  readonly childField: CompoundKey;
  /**
   * The same columns as the aggregate rows name them (see
   * {@link BuilderDelegate.aggregateKeyColumns}), in the same order.
   */
  readonly keyField: CompoundKey;
  readonly fn: AggregateFunction;
  readonly field: string | undefined;
  /**
   * The child `where` compiled to a per-row predicate, or `undefined` when the
   * aggregate has no `where` (every child contributes).
   */
  readonly predicate: ((row: Row) => boolean) | undefined;
};

/**
 * Deterministic synthetic table name for an `aggregate` relationship. Must
 * agree between the server (which produces the rows) and the synced client
 * (which reads them). It folds in the `queryID` — the client query hash, which
 * both sides share and which already distinguishes differently-filtered queries
 * — and the relationship's alias path from the root, so two aggregates never
 * collide, even ones with the same alias at different depths.
 *
 * @param parentName The pipeline name of the parent, which the builder forms
 *        from the aliases of the relationships leading to it (`''` at the
 *        root, `'.comments'` one level down). It is the same on both sides.
 */
export function aggregateTableName(
  queryID: string,
  sq: CorrelatedSubquery,
  parentName = '',
): string {
  const path = childPipelineName(parentName, sq).slice(1);
  return `${AGGREGATE_TABLE_PREFIX}${queryID}${AGGREGATE_TABLE_SEPARATOR}${path}`;
}

/**
 * The pipeline name of the child `sq` under the pipeline `name`: the parent's
 * name, a `.`, and the escaped alias. A relationship name can be any string, so
 * the alias is escaped to keep the `.`-joined path unambiguous and free of the
 * `/` and `:` that {@link aggregateTableName} must not contain.
 */
function childPipelineName(name: string, sq: CorrelatedSubquery): string {
  const alias = encodeURIComponent(sq.subquery.alias ?? '').replaceAll(
    '.',
    '%2E',
  );
  return `${name}.${alias}`;
}

/**
 * Deterministic synthetic table name for a *top-level* (ungrouped) aggregate
 * query (e.g. `z.query.issue.count()`). There is no relationship alias, so the
 * name is just the prefix + `queryID` (`aggregate:<queryID>`). Distinct from
 * relationship-aggregate names (`aggregate:<queryID>:<alias>`), which always
 * carry an alias segment — which is how the client tells the two apart by name.
 */
export function topLevelAggregateTableName(queryID: string): string {
  return `${AGGREGATE_TABLE_PREFIX}${queryID}`;
}

/** True if `name` is a synthetic aggregate table (any kind). */
export function isAggregateTableName(name: string): boolean {
  return name.startsWith(AGGREGATE_TABLE_PREFIX);
}

/**
 * True if `name` is a *top-level* aggregate table (`aggregate:<queryID>`), as
 * opposed to a relationship one (`aggregate:<queryID>:<alias>`). Top-level names
 * have no further separator after the prefix. The client uses this to recognize
 * a synthetic source it can provision from the name alone (fixed key + value
 * shape), which relationship aggregates — needing the correlation key — cannot.
 */
export function isTopLevelAggregateTableName(name: string): boolean {
  return (
    name.startsWith(AGGREGATE_TABLE_PREFIX) &&
    !name
      .slice(AGGREGATE_TABLE_PREFIX.length)
      .includes(AGGREGATE_TABLE_SEPARATOR)
  );
}

function uniquifyCorrelatedSubqueryConditionAliases(ast: AST): AST {
  if (!ast.where) {
    return ast;
  }
  const {where} = ast;
  if (where.type !== 'and' && where.type !== 'or') {
    return ast;
  }

  let count = 0;
  const uniquifyCorrelatedSubquery = (csqc: CorrelatedSubqueryCondition) => ({
    ...csqc,
    related: {
      ...csqc.related,
      subquery: {
        ...csqc.related.subquery,
        alias: (csqc.related.subquery.alias ?? '') + '_' + count++,
      },
    },
  });

  const uniquify = (cond: Condition): Condition => {
    if (cond.type === 'simple') {
      return cond;
    } else if (cond.type === 'correlatedSubquery') {
      return uniquifyCorrelatedSubquery(cond);
    }
    const conditions = [];
    for (const c of cond.conditions) {
      conditions.push(uniquify(c));
    }
    return {
      type: cond.type,
      conditions,
    };
  };

  const result = {
    ...ast,
    where: uniquify(where),
  };
  return result;
}

export function conditionIncludesFlippedSubqueryAtAnyLevel(
  cond: Condition,
): boolean {
  if (cond.type === 'correlatedSubquery') {
    return !!cond.flip;
  }
  if (cond.type === 'and' || cond.type === 'or') {
    return cond.conditions.some(c =>
      conditionIncludesFlippedSubqueryAtAnyLevel(c),
    );
  }
  // simple conditions don't have flips
  return false;
}

export function partitionBranches(
  conditions: readonly Condition[],
  predicate: (c: Condition) => boolean,
) {
  const matched: Condition[] = [];
  const notMatched: Condition[] = [];
  for (const c of conditions) {
    if (predicate(c)) {
      matched.push(c);
    } else {
      notMatched.push(c);
    }
  }
  return [matched, notMatched] as const;
}
