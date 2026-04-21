import type {LogContext} from '@rocicorp/logger';
import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {JSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {
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
import {InputIntersection, InputUnion} from '../ivm/set-operators.ts';
import {Skip} from '../ivm/skip.ts';
import type {Source, SourceInput} from '../ivm/source.ts';
import {StripRelationships} from '../ivm/strip-relationships.ts';
import {Take} from '../ivm/take.ts';
import {UnionFanIn} from '../ivm/union-fan-in.ts';
import {UnionFanOut} from '../ivm/union-fan-out.ts';
import {planQuery} from '../planner/planner-builder.ts';
import type {ConnectionCostModel} from '../planner/planner-connection.ts';
import type {PlannerConstraint} from '../planner/planner-constraint.ts';
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
   * Called once for each source needed by the AST.
   * Might be called multiple times with same tableName. It is OK to return
   * same storage instance in that case.
   */
  getSource(tableName: string): Source | undefined;

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
  ast = completeOrdering(
    ast,
    tableName => must(delegate.getSource(tableName)).tableSchema.primaryKey,
  );

  if (costModel) {
    ast = planQuery(ast, costModel, planDebugger, lc);
  }
  return buildPipelineInternal(
    ast,
    delegate,
    queryID,
    '',
    undefined,
    undefined,
    costModel,
  );
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
const SIBLING_INTERSECTION_COST_FUZZ_FACTOR = 4;

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
  costModel?: ConnectionCostModel,
): Input {
  const source = delegate.getSource(ast.table);
  if (!source) {
    throw new Error(`Source not found: ${ast.table}`);
  }

  ast = uniquifyCorrelatedSubqueryConditionAliases(ast);

  if (!delegate.enableNotExists && ast.where) {
    assertNoNotExists(ast.where);
  }

  // Two narrow physical rewrites run before the generic source/filter/join
  // pipeline below. These do not change the meaning of the WHERE clause. They
  // only choose a better place to start reading rows.
  //
  // OR example:
  //
  //   Query:
  //
  //     item
  //       |-- status = 'open'
  //       `-- OR EXISTS(item_tag WHERE tag = 'bug')
  //
  //   Scan plan:
  //
  //     item(status = 'open') -----------------------.
  //                                                   +-- union item.id
  //     item_tag(tag = 'bug') -> item(id) -----------'
  //
  // The broad plan would scan all items and ask "does either branch match?"
  // for every row. This plan starts from both selective doorways into item
  // rows, then dedupes by item.id.
  //
  // AND example:
  //
  //   Query:
  //
  //     item
  //       |-- EXISTS(item_tag WHERE tag = 'bug')
  //       `-- AND EXISTS(item_watch WHERE user_id = 7)
  //
  //   Scan plan:
  //
  //     item_tag(tag = 'bug')      -> item_ids {10, 20} --.
  //                                                            +-- ids in both -> item(id)
  //     item_watch(user_id = 7)    -> item_ids {20, 30} ----'
  //
  // The broad plan would load items after the first child match, then probe
  // the second child row-by-row. This plan first finds the item ids that
  // appear in both child scans, then loads only those items.
  //
  // Each rewrite has its own strict guard below. If a query needs a shape the
  // new physical operator cannot preserve, it falls through to the older
  // generic pipeline.
  const rootUnionBranches = getRootUnionBranches(ast);
  if (rootUnionBranches) {
    return applyRootUnionBranches(
      ast,
      rootUnionBranches,
      delegate,
      queryID,
      name,
      partitionKey,
      costModel,
    );
  }

  const intersection = getSiblingExistsIntersection(
    ast.where,
    delegate,
    costModel,
  );
  const sourceWhere = intersection ? intersection.sourceWhere : ast.where;
  const csqConditions = intersection
    ? []
    : gatherCorrelatedSubqueryQueryConditions(ast.where);
  const splitEditKeys: Set<string> = partitionKey
    ? new Set(partitionKey)
    : new Set();
  for (const csq of csqConditions) {
    for (const key of csq.related.correlation.parentField) {
      splitEditKeys.add(key);
    }
  }
  if (intersection) {
    for (const key of intersection.related.correlation.parentField) {
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
    sourceWhere,
    splitEditKeys,
    delegate.debug,
  );

  let end: Input = delegate.decorateSourceInput(conn, queryID);
  end = delegate.decorateInput(end, `${name}:source(${ast.table})`);
  const {fullyAppliedFilters} = conn;

  if (intersection) {
    end = applySiblingExistsIntersection(
      intersection,
      delegate,
      end,
      name,
      costModel,
    );
  }

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
        costModel,
      );
    }
  }

  if (sourceWhere && (!fullyAppliedFilters || delegate.applyFiltersAnyway)) {
    end = applyWhere(end, sourceWhere, delegate, name, costModel);
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
      end = applyCorrelatedSubQuery(
        csq,
        delegate,
        queryID,
        end,
        name,
        false,
        costModel,
      );
    }
  }

  return end;
}

function applyRootUnionBranches(
  ast: AST,
  branches: readonly Condition[],
  delegate: BuilderDelegate,
  queryID: string,
  name: string,
  partitionKey?: CompoundKey,
  costModel?: ConnectionCostModel,
): Input {
  // Run every OR branch as its own root query, then merge by primary key. A
  // root is just the table/index we choose to scan first. This is the physical
  // equivalent of SQLite's multi-index OR strategy:
  //
  //   Query:
  //
  //     item.status = 'open'
  //       OR EXISTS(item_tag WHERE tag = 'bug')
  //
  //   Scan plan:
  //
  //     item(status = 'open') -----------------------.
  //                                                   +-- union item.id
  //     item_tag(tag = 'bug') -> item(id) -----------'
  //
  // Each recursive branch keeps the same ordering and split-edit keys as the
  // original AST, so the union can merge streams without re-sorting.
  const inputs = branches.map((branch, index) => {
    const input = buildPipelineInternal(
      {
        ...ast,
        where: branch,
      },
      delegate,
      queryID,
      `${name}:or-${index}`,
      partitionKey,
      undefined,
      costModel,
    );
    return stripRootUnionBranchRelationships(input, delegate, name, index);
  });

  const union = new InputUnion(inputs);
  for (const input of inputs) {
    delegate.addEdge(input, union);
  }
  return delegate.decorateInput(union, `${name}:input-union`);
}

function stripRootUnionBranchRelationships(
  input: Input,
  delegate: BuilderDelegate,
  name: string,
  index: number,
): Input {
  // Root union is a set of parent rows, not a way to expose relationship
  // payloads. A flipped EXISTS branch may temporarily attach its child rows so
  // the branch can prove the EXISTS is true, but those rows are condition-only
  // data. Strip them before the branch joins the union so every branch exposes
  // the same plain parent-row schema.
  if (Object.keys(input.getSchema().relationships).length === 0) {
    return input;
  }
  const stripped = new StripRelationships(input);
  delegate.addEdge(input, stripped);
  return delegate.decorateInput(stripped, `${name}:or-${index}:strip-related`);
}

function getRootUnionBranches(ast: AST): readonly Condition[] | undefined {
  // This rewrite is only safe at the root of a plain query. start, limit, and
  // related rows all observe the whole result stream, so they need a richer
  // physical plan than "run branch pipelines, then union".
  if (
    ast.where === undefined ||
    ast.start !== undefined ||
    ast.limit !== undefined ||
    ast.related !== undefined
  ) {
    return undefined;
  }

  const branches = getRootUnionBranchConditions(ast.where);
  if (!branches || branches.length < 2) {
    return undefined;
  }

  // At least one branch must already be planned to start from a related table.
  // Otherwise root union would just split a simple parent-table scan into
  // smaller scans without buying us anything.
  if (!branches.some(conditionIncludesFlippedSubqueryAtAnyLevel)) {
    return undefined;
  }

  // There must also be at least one local parent branch. If every branch is a
  // child branch, the existing UnionFanOut and UnionFanIn path handles it.
  if (!branches.some(isNotAndDoesNotContainSubquery)) {
    return undefined;
  }

  // The normalizer flattens ORs before planning. If a nested OR survives here,
  // keep the old path rather than inventing branch semantics locally.
  if (branches.some(branch => branch.type === 'or')) {
    return undefined;
  }

  return branches;
}

function getRootUnionBranchConditions(
  condition: Condition,
): readonly Condition[] | undefined {
  if (condition.type === 'or') {
    return condition.conditions;
  }
  if (condition.type !== 'and') {
    return undefined;
  }

  const orBranches: Disjunction[] = [];
  const shared: Condition[] = [];
  for (const subCondition of condition.conditions) {
    if (subCondition.type === 'or') {
      orBranches.push(subCondition);
      continue;
    }
    if (!isNotAndDoesNotContainSubquery(subCondition)) {
      return undefined;
    }
    shared.push(subCondition);
  }

  if (orBranches.length !== 1) {
    return undefined;
  }

  // Physical distributivity for the common permission shape:
  //
  //   shared_parent_filter AND (local_branch OR child_branch)
  //            |
  //            v
  //   (shared_parent_filter AND local_branch)
  //     OR
  //   (shared_parent_filter AND child_branch)
  //
  // This lets the local branch use all of its parent filters at the source
  // instead of first scanning the broad shared filter and applying the local
  // branch in a later Filter operator.
  return orBranches[0].conditions.map(branch =>
    combineConditions([...shared, branch]),
  );
}

function applySiblingExistsIntersection(
  intersection: SiblingExistsIntersection,
  delegate: BuilderDelegate,
  end: Input,
  name: string,
  costModel?: ConnectionCostModel,
): Input {
  // Build the child side before the parent lookup. In plain English:
  // find the parent ids that satisfy every sibling EXISTS first, then load the
  // parent rows for only those ids.
  //
  //   Query:
  //
  //     item
  //       |-- EXISTS(item_tag WHERE tag = 'bug')
  //       `-- EXISTS(item_watch WHERE user_id = 7)
  //
  //   Scan plan:
  //
  //     item_tag(tag = 'bug') ----------------------.
  //                                                  +-- intersect item ids -> item(id)
  //     item_watch(user_id = 7) --------------------'
  //
  // This avoids loading an item after the first child match only to probe the
  // second child relationship row-by-row.
  const {conditions, related} = intersection;
  const childInputs = conditions.map((condition, index) =>
    buildPipelineInternal(
      condition.related.subquery,
      delegate,
      '',
      `${name}.${condition.related.subquery.alias}:intersect-${index}`,
      condition.related.correlation.childField,
      undefined,
      costModel,
    ),
  );
  const child = new InputIntersection(
    childInputs,
    conditions[0].related.correlation.childField,
    conditions.map(condition => condition.related.correlation.childField),
  );
  for (const childInput of childInputs) {
    delegate.addEdge(childInput, child);
  }

  const flippedJoin = new FlippedJoin({
    parent: end,
    child,
    parentKey: related.correlation.parentField,
    childKey: related.correlation.childField,
    relationshipName: must(
      related.subquery.alias,
      'Subquery must have an alias',
    ),
    hidden: related.hidden ?? false,
    system: related.system ?? 'client',
  });
  delegate.addEdge(end, flippedJoin);
  delegate.addEdge(child, flippedJoin);
  return delegate.decorateInput(
    flippedJoin,
    `${name}:intersect-flipped-join(${related.subquery.alias})`,
  );
}

type SiblingExistsIntersection = {
  readonly related: CorrelatedSubquery;
  readonly conditions: readonly CorrelatedSubqueryCondition[];
  readonly sourceWhere: Condition | undefined;
};

function getSiblingExistsIntersection(
  condition: Condition | undefined,
  delegate: BuilderDelegate,
  costModel?: ConnectionCostModel,
): SiblingExistsIntersection | undefined {
  // Detect a narrow, physical intersection opportunity:
  //
  //   AND
  //     local parent filters...
  //     EXISTS(child relationship A)
  //     EXISTS(child relationship B)
  //
  // We only need one sibling to be planned as flipped. Once one branch is
  // source-driven, intersecting all compatible siblings lets the runtime start
  // with child tables and avoid parent-first probing.
  if (condition?.type !== 'and') {
    return undefined;
  }

  const candidates: CorrelatedSubqueryCondition[] = [];
  const localConditions: NoSubqueryCondition[] = [];
  for (const subCondition of condition.conditions) {
    const exists = getIntersectableExists(subCondition);
    if (exists) {
      candidates.push(exists);
      continue;
    }
    if (isNotAndDoesNotContainSubquery(subCondition)) {
      localConditions.push(subCondition);
      continue;
    }
    return undefined;
  }

  if (candidates.length < 2) {
    return undefined;
  }

  // Respect explicit user intent. flip: false means "keep this semi-join".
  // Undefined still means the planner may decide, so it can join a group where
  // another sibling was chosen as flipped.
  if (!candidates.some(candidate => candidate.flip === true)) {
    return undefined;
  }

  const fingerprint = siblingExistsIntersectionFingerprint(candidates[0]);
  if (
    !fingerprint ||
    candidates.some(
      candidate =>
        siblingExistsIntersectionFingerprint(candidate) !== fingerprint,
    )
  ) {
    return undefined;
  }

  // InputIntersection works with ids, not duplicate child rows: "which parent
  // ids appeared in every related-table scan?" Because the operator keeps one
  // child row for each parent id, each branch must prove it can produce at
  // most one child row for the id it contributes.
  //
  //   child table primary key:
  //
  //     [parent_id, tag]
  //
  //   Branch filter:
  //
  //     parent_id comes from the relationship
  //     tag = 'bug'
  //
  // Together those two values cover the full primary key, so this branch can
  // produce at most one child row for each parent id.
  for (const candidate of candidates) {
    const childSource = delegate.getSource(candidate.related.subquery.table);
    if (!childSource) {
      return undefined;
    }
    if (
      !isUniquePerCorrelationKey(candidate, childSource.tableSchema.primaryKey)
    ) {
      return undefined;
    }
  }

  if (!isSiblingIntersectionCostSafe(candidates, costModel)) {
    return undefined;
  }

  return {
    related: candidates[0].related,
    conditions: candidates,
    sourceWhere: combineAndConditions(localConditions),
  };
}

function isSiblingIntersectionCostSafe(
  candidates: readonly CorrelatedSubqueryCondition[],
  costModel: ConnectionCostModel | undefined,
): boolean {
  if (!costModel) {
    return true;
  }

  const childScanScores = candidates.map(candidate =>
    siblingExistsCostScore(candidate, costModel, undefined),
  );
  const flippedIndexes = candidates.flatMap((candidate, index) =>
    candidate.flip === true ? [index] : [],
  );
  if (flippedIndexes.length === 0) {
    return false;
  }

  const driverRows = Math.max(
    1,
    Math.min(...flippedIndexes.map(index => childScanScores[index].rows)),
  );
  const driverScore = Math.min(
    ...flippedIndexes.map(index => childScanScores[index].score),
  );
  const semiJoinProbeScore = candidates.reduce((total, candidate) => {
    if (candidate.flip === true) {
      return total;
    }
    return (
      total +
      driverRows *
        siblingExistsCostScore(
          candidate,
          costModel,
          childCorrelationConstraint(candidate),
        ).score
    );
  }, 0);

  const currentPlanScore = driverScore + semiJoinProbeScore;
  const intersectionScore = childScanScores.reduce(
    (total, cost) => total + cost.score,
    0,
  );

  // This is only a guardrail around a physical shortcut. The planner has
  // already chosen at least one child-driven branch. Intersection is great when
  // sibling child scans are in the same ballpark, but it should not let one tiny
  // flipped branch drag a whole broad child table into memory. The fuzz factor
  // gives SQLite's approximate row estimates room to be imperfect while still
  // rejecting the pathological "3 row branch AND 30k row branch" shape.
  return (
    intersectionScore <=
    currentPlanScore * SIBLING_INTERSECTION_COST_FUZZ_FACTOR
  );
}

function siblingExistsCostScore(
  condition: CorrelatedSubqueryCondition,
  costModel: ConnectionCostModel,
  constraint: PlannerConstraint | undefined,
): {readonly rows: number; readonly score: number} {
  const {subquery} = condition.related;
  const cost = costModel(
    subquery.table,
    subquery.orderBy ?? [],
    subquery.where,
    constraint,
  );
  return {rows: cost.rows, score: cost.rows + cost.startupCost};
}

function childCorrelationConstraint(
  condition: CorrelatedSubqueryCondition,
): PlannerConstraint {
  return Object.fromEntries(
    condition.related.correlation.childField.map(field => [field, undefined]),
  );
}

function getIntersectableExists(
  condition: Condition,
): CorrelatedSubqueryCondition | undefined {
  // The AND intersection rewrite is only valid for this simple shape:
  //
  //   EXISTS(related table)
  //     where related-table filters have no nested EXISTS
  //     with no related rows, cursor start, or limit inside the EXISTS
  //
  // Nested relationships, cursors, and limits can make "does this parent key
  // exist?" depend on more than the related table's filter. In that world,
  // intersecting related-table ids could skip rows that the original sibling
  // EXISTS checks would have accepted.
  const exists = asCorrelatedSubqueryCondition(condition);
  if (!exists) {
    return undefined;
  }
  if (!isPlainExistsBranch(exists)) {
    return undefined;
  }
  if (!hasIntersectableChildSubquery(exists)) {
    return undefined;
  }
  return exists;
}

function asCorrelatedSubqueryCondition(
  condition: Condition,
): CorrelatedSubqueryCondition | undefined {
  return condition.type === 'correlatedSubquery' ? condition : undefined;
}

function isPlainExistsBranch(condition: CorrelatedSubqueryCondition): boolean {
  return (
    condition.op === 'EXISTS' &&
    condition.scalar !== true &&
    condition.flip !== false
  );
}

function hasIntersectableChildSubquery(
  condition: CorrelatedSubqueryCondition,
): boolean {
  const {subquery} = condition.related;
  return (
    subquery.related === undefined &&
    subquery.start === undefined &&
    subquery.limit === undefined &&
    (subquery.where === undefined ||
      isNotAndDoesNotContainSubquery(subquery.where))
  );
}

function siblingExistsIntersectionFingerprint(
  condition: CorrelatedSubqueryCondition,
): string | undefined {
  const exists = getIntersectableExists(condition);
  if (!exists) {
    return undefined;
  }

  const {related} = exists;
  // Different child tables can participate as long as they all map their key
  // back to the same parent key. The intersection keeps the first child input
  // as the representative row stream and uses the rest only as key sets:
  //
  //   child A(parent_id) ----.
  //                           +-- ids in every branch -> parent(id)
  //   child B(parent_id) ----'
  //
  // Because non-representative inputs only contribute keys, their table name,
  // schema, and sort order do not need to match the first input.
  return JSON.stringify({
    system: related.system,
    hidden: related.hidden,
    parentField: related.correlation.parentField,
    keyWidth: related.correlation.childField.length,
  });
}

function combineAndConditions(
  conditions: readonly NoSubqueryCondition[],
): Condition | undefined {
  if (conditions.length === 0) {
    return undefined;
  }
  if (conditions.length === 1) {
    return conditions[0];
  }
  return combineConditions(conditions);
}

function combineConditions(conditions: readonly Condition[]): Condition {
  if (conditions.length === 1) {
    return conditions[0];
  }
  return {type: 'and', conditions: [...conditions]};
}

function isUniquePerCorrelationKey(
  condition: CorrelatedSubqueryCondition,
  childPrimaryKey: readonly string[],
): boolean {
  // Prove that a child scan contributes at most one row per parent id:
  //
  //   parent id field + literal equality filters cover child PK
  //
  // Example:
  //
  //   child table primary key:
  //
  //     [parent_id, tag]
  //
  //   Branch filter:
  //
  //     parent_id comes from the relationship
  //     tag = 'bug'
  //
  // Together those values identify one child row. That makes intersecting by
  // parent_id equivalent to asking whether all sibling EXISTS branches
  // are true.
  const constrained = new Set(condition.related.correlation.childField);
  collectEqualityConstrainedColumns(
    condition.related.subquery.where,
    constrained,
  );
  return childPrimaryKey.every(key => constrained.has(key));
}

function collectEqualityConstrainedColumns(
  condition: Condition | undefined,
  constrained: Set<string>,
): void {
  // This is intentionally conservative. Only literal equality predicates prove
  // uniqueness. IN, OR, scalar subqueries, and nested EXISTS can still be
  // optimized later, but they need a richer proof than this small helper.
  if (!condition) {
    return;
  }
  if (condition.type === 'simple') {
    if (
      condition.op === '=' &&
      condition.left.type === 'column' &&
      condition.right.type === 'literal'
    ) {
      constrained.add(condition.left.name);
    }
    return;
  }
  if (condition.type === 'and') {
    for (const child of condition.conditions) {
      collectEqualityConstrainedColumns(child, constrained);
    }
  }
}

function applyWhere(
  input: Input,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
  costModel?: ConnectionCostModel,
): Input {
  if (!conditionIncludesFlippedSubqueryAtAnyLevel(condition)) {
    return buildFilterPipeline(input, delegate, filterInput =>
      applyFilter(filterInput, condition, delegate, name, costModel),
    );
  }

  return applyFilterWithFlips(input, condition, delegate, name, costModel);
}

function applyFilterWithFlips(
  input: Input,
  condition: Condition,
  delegate: BuilderDelegate,
  name: string,
  costModel?: ConnectionCostModel,
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
            costModel,
          ),
        );
      }
      assert(withFlipped.length > 0, 'Impossible to have no flips here');
      for (const cond of withFlipped) {
        end = applyFilterWithFlips(end, cond, delegate, name, costModel);
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
        const branch = buildFilterPipeline(end, delegate, filterInput =>
          applyOr(
            filterInput,
            {
              type: 'or',
              conditions: withoutFlipped,
            },
            delegate,
            name,
            costModel,
          ),
        );
        branches.push(branch);
      }

      for (const cond of withFlipped) {
        branches.push(
          applyFilterWithFlips(end, cond, delegate, name, costModel),
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
        `${name}.${sq.subquery.alias}`,
        sq.correlation.childField,
        false,
        costModel,
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
  costModel?: ConnectionCostModel,
): FilterInput {
  switch (condition.type) {
    case 'and':
      return applyAnd(input, condition, delegate, name, costModel);
    case 'or':
      return applyOr(input, condition, delegate, name, costModel);
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
  costModel?: ConnectionCostModel,
): FilterInput {
  for (const subCondition of condition.conditions) {
    input = applyFilter(input, subCondition, delegate, name, costModel);
  }
  return input;
}

export function applyOr(
  input: FilterInput,
  condition: Disjunction,
  delegate: BuilderDelegate,
  name: string,
  costModel?: ConnectionCostModel,
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
    applyFilter(fanOut, subCondition, delegate, name, costModel),
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
  costModel?: ConnectionCostModel,
) {
  // TODO: we only omit the join if the CSQ if from a condition since
  // we want to create an empty array for `related` fields that are `limit(0)`
  if (sq.subquery.limit === 0 && fromCondition) {
    return end;
  }

  assert(sq.subquery.alias, 'Subquery must have an alias');
  const child = buildPipelineInternal(
    sq.subquery,
    delegate,
    queryID,
    `${name}.${sq.subquery.alias}`,
    sq.correlation.childField,
    fromCondition,
    costModel,
  );

  const joinName = `${name}:join(${sq.subquery.alias})`;
  const join = new Join({
    parent: end,
    child,
    parentKey: sq.correlation.parentField,
    childKey: sq.correlation.childField,
    relationshipName: sq.subquery.alias,
    hidden: sq.hidden ?? false,
    system: sq.system ?? 'client',
  });
  delegate.addEdge(end, join);
  delegate.addEdge(child, join);
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
