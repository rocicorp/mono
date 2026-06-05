import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
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
  Ordering,
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
import {Join} from '../ivm/join.ts';
import type {Input, InputBase, Storage} from '../ivm/operator.ts';
import {Skip} from '../ivm/skip.ts';
import type {Source, SourceInput} from '../ivm/source.ts';
import {Take} from '../ivm/take.ts';
import {UnionFanIn} from '../ivm/union-fan-in.ts';
import {UnionFanOut} from '../ivm/union-fan-out.ts';
import {planQuery} from '../planner/planner-builder.ts';
import type {ConnectionCostModel} from '../planner/planner-connection.ts';
import type {PlanDebugger} from '../planner/planner-debug.ts';
import {completeOrdering} from '../query/complete-ordering.ts';
import type {DebugDelegate} from './debug-delegate.ts';
import {createPredicate, type NoSubqueryCondition} from './filter.ts';

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
   * When true, an `aggregate` relationship is read from a synthetic,
   * pre-computed aggregate source (see {@linkcode aggregateTableName}) via a
   * plain Join, instead of being computed by an {@linkcode Aggregate} operator
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
     * child row in `table` is locally mutated (only supplied for the invertible,
     * where-free cases — see the call site). Omitted ⇒ server-authoritative only.
     */
    optimisticDelta?: {
      readonly table: string;
      readonly childField: CompoundKey;
      readonly fn: AggregateFunction;
      readonly field: string | undefined;
    },
  ): Source;

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

  if (costModel) {
    ast = planQuery(ast, costModel, planDebugger, lc);
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
export function assertNoNotExists(condition: Condition): void {
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
  isNonFlippedExistsChild?: boolean | undefined,
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

  for (const csqCondition of csqConditions) {
    // flipped EXISTS are handled in applyWhere
    if (!csqCondition.flip) {
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
      );
    }
  }

  if (ast.where && (!fullyAppliedFilters || delegate.applyFiltersAnyway)) {
    end = applyWhere(end, ast.where, delegate, name);
  }

  if (ast.aggregate) {
    // Top-level (ungrouped) aggregate, compute mode (server / local): the
    // Aggregate operator reduces the filtered rows to one synthetic row emitted
    // to `aggregate:<queryID>`. orderBy / limit / related do not apply to a
    // scalar result. (The synced client takes the aggregatesFromSource
    // short-circuit at the top of this function instead.)
    const aggName = `${name}:aggregate`;
    const aggregate = new Aggregate(
      end,
      delegate.createStorage(aggName),
      [], // ungrouped — one global group
      ast.aggregate.fn,
      ast.aggregate.field,
      topLevelAggregateTableName(queryID),
    );
    delegate.addEdge(end, aggregate);
    return delegate.decorateInput(aggregate, aggName);
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
    }
  }

  if (ast.related) {
    // Dedupe by alias - last one wins (LWW), like limit(5).limit(10)
    const byAlias = new Map<string, CorrelatedSubquery>();
    for (const csq of ast.related) {
      byAlias.set(csq.subquery.alias ?? '', csq);
    }
    for (const csq of byAlias.values()) {
      end = applyCorrelatedSubQuery(csq, delegate, queryID, end, name, false);
    }
  }

  return end;
}

function applyWhere(
  input: Input,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
): Input {
  if (!conditionIncludesFlippedSubqueryAtAnyLevel(condition)) {
    return buildFilterPipeline(input, delegate, filterInput =>
      applyFilter(filterInput, condition, delegate, name),
    );
  }

  return applyFilterWithFlips(input, condition, delegate, name);
}

function applyFilterWithFlips(
  input: Input,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
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
        end = buildFilterPipeline(input, delegate, filterInput =>
          applyAnd(
            filterInput,
            {
              type: 'and',
              conditions: withoutFlipped,
            },
            delegate,
            name,
          ),
        );
      }
      assert(withFlipped.length > 0, 'Impossible to have no flips here');
      for (const cond of withFlipped) {
        end = applyFilterWithFlips(end, cond, delegate, name);
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
        branches.push(
          buildFilterPipeline(end, delegate, filterInput =>
            applyOr(
              filterInput,
              {
                type: 'or',
                conditions: withoutFlipped,
              },
              delegate,
              name,
            ),
          ),
        );
      }

      for (const cond of withFlipped) {
        branches.push(applyFilterWithFlips(end, cond, delegate, name));
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
        `${name}.${sq.subquery.alias}`,
        sq.correlation.childField,
        false,
      );
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
      });
      delegate.addEdge(end, flippedJoin);
      delegate.addEdge(child, flippedJoin);
      end = delegate.decorateInput(
        flippedJoin,
        `${name}:flipped-join(${sq.subquery.alias})`,
      );
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

export function applyOr(
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

export function isNotAndDoesNotContainSubquery(
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
) {
  // TODO: we only omit the join if the CSQ if from a condition since
  // we want to create an empty array for `related` fields that are `limit(0)`
  if (sq.subquery.limit === 0 && fromCondition) {
    return end;
  }

  assert(sq.subquery.alias, 'Subquery must have an alias');

  // PROTOTYPE: an `aggregate` relationship (count/sum/avg).
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
  if (sq.aggregate) {
    const aggTable = aggregateTableName(queryID, sq);
    if (delegate.aggregatesFromSource) {
      let source = delegate.getSource(aggTable);
      if (!source && delegate.getAggregateSource) {
        // The synthetic source isn't a schema table; provision it from the
        // child's column types + the aggregate fn/field so the synced rows have
        // a correctly-shaped place to land (key = correlation child field).
        const childSource = delegate.getSource(sq.subquery.table);
        if (!childSource) {
          throw new Error(`Source not found: ${sq.subquery.table}`);
        }
        const {columns} = aggregateSourceSchema(
          childSource.tableSchema.columns,
          sq.correlation.childField,
          sq.aggregate.fn,
          sq.aggregate.field,
        );
        source = delegate.getAggregateSource(
          aggTable,
          columns,
          sq.correlation.childField,
          // Optimistic deltas are only safe for the invertible, where-free case:
          // `count` (±1), `sum`/`avg` (±field) per child add/remove. A `where` on
          // the child would require evaluating the predicate per row, and
          // `min`/`max` aren't invertible — those fall back to server values.
          (sq.aggregate.fn === 'count' ||
            sq.aggregate.fn === 'sum' ||
            sq.aggregate.fn === 'avg') &&
            sq.subquery.where === undefined
            ? {
                table: sq.subquery.table,
                childField: sq.correlation.childField,
                fn: sq.aggregate.fn,
                field: sq.aggregate.field,
              }
            : undefined,
        );
      }
      if (!source) {
        throw new Error(`Aggregate source not found: ${aggTable}`);
      }
      const conn = source.connect(
        sq.correlation.childField.map(f => [f, 'asc'] as const),
      );
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
        `${name}.${sq.subquery.alias}`,
        sq.correlation.childField,
        fromCondition,
      );
      const aggName = `${name}:aggregate(${sq.subquery.alias})`;
      const aggregate = new Aggregate(
        child,
        delegate.createStorage(aggName),
        sq.correlation.childField,
        sq.aggregate.fn,
        sq.aggregate.field,
        aggTable,
      );
      delegate.addEdge(child, aggregate);
      childInput = delegate.decorateInput(aggregate, aggName);
    }
  } else {
    childInput = buildPipelineInternal(
      sq.subquery,
      delegate,
      queryID,
      `${name}.${sq.subquery.alias}`,
      sq.correlation.childField,
      fromCondition,
    );
  }

  const joinName = `${name}:join(${sq.subquery.alias})`;
  const join = new Join({
    parent: end,
    child: childInput,
    parentKey: sq.correlation.parentField,
    childKey: sq.correlation.childField,
    relationshipName: sq.subquery.alias,
    hidden: sq.hidden ?? false,
    system: sq.system ?? 'client',
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

/**
 * Prefix for the synthetic table that holds the result rows of an `aggregate`
 * query/relationship. The synthetic name keeps aggregate rows distinct from real
 * child rows when routed/synced.
 *
 * The separator is `:` rather than `/` on purpose: synthetic table names must
 * contain **no `/`** so they round-trip through the client's Replicache row-key
 * format (`e/<table>/<pk>`) and {@link sourceNameFromKey} unchanged — those
 * split on the first `/`, which must be the table↔key delimiter. Real table
 * names are SQL identifiers and never contain `:`, so collisions are impossible.
 */
export const AGGREGATE_TABLE_SEPARATOR = ':';
export const AGGREGATE_TABLE_PREFIX = `aggregate${AGGREGATE_TABLE_SEPARATOR}`;

/**
 * Deterministic synthetic table name for an `aggregate` relationship. Must
 * agree between the server (which produces the rows) and the synced client
 * (which reads them). It folds in the `queryID` — the client query hash, which
 * both sides share and which already distinguishes differently-filtered queries
 * — and the relationship alias, so two aggregates never collide.
 */
export function aggregateTableName(
  queryID: string,
  sq: CorrelatedSubquery,
): string {
  return `${AGGREGATE_TABLE_PREFIX}${queryID}${AGGREGATE_TABLE_SEPARATOR}${sq.subquery.alias}`;
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

export function assertOrderingIncludesPK(
  ordering: Ordering,
  pk: PrimaryKey,
): void {
  // oxlint-disable-next-line unicorn/prefer-set-has -- Array is more appropriate here for small collections
  const orderingFields = ordering.map(([field]) => field);
  const missingFields = pk.filter(pkField => !orderingFields.includes(pkField));

  if (missingFields.length > 0) {
    throw new Error(
      `Ordering must include all primary key fields. Missing: ${missingFields.join(
        ', ',
      )}. ZQL automatically appends primary key fields to the ordering if they are missing 
      so a common cause of this error is a casing mismatch between Postgres and ZQL.
      E.g., "userid" vs "userID".
      You may want to add double-quotes around your Postgres column names to prevent Postgres from lower-casing them:
      https://www.postgresql.org/docs/current/sql-syntax-lexical.htm`,
    );
  }
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
