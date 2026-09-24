import type {Condition, Ordering} from '../../../zero-protocol/src/ast.ts';
import type {Plans} from './planner-builder.ts';
import type {
  AccessPlan,
  ConnectionAccess,
  PlannerConnection,
} from './planner-connection.ts';

/**
 * When the planner reports a {@link PlanWarning}. Each threshold is compared
 * against the cost model's estimates, and 0 disables the warnings it gates.
 */
export type PlanWarningThresholds = {
  /**
   * The number of rows a single read of a table must scan (for a
   * {@link MissingIndexWarning}) or sort (for a {@link FullSortWarning}).
   */
  readonly rows: number;

  /**
   * The number of rows the chosen plan must be estimated to process, for a
   * {@link HighCostWarning}.
   */
  readonly cost: number;
};

type TableRead = {
  readonly table: string;

  /**
   * The aliases of the `related` subqueries leading to the read, starting
   * from the root query. Empty for the root query and its EXISTS subqueries.
   */
  readonly path: readonly string[];

  /**
   * Whether the read runs once per row of another table, because a join
   * binds some of its columns, rather than once per query.
   */
  readonly perRow: boolean;

  /**
   * An index that would let the source find the rows it reads and return
   * them in order: the columns the rows are found by, then the ordering.
   */
  readonly suggestedIndex: readonly string[];
};

/**
 * The rows of a table are found by columns that no index covers, so every
 * read scans the whole table. This is typically a relationship whose foreign
 * key is not indexed.
 */
export type MissingIndexWarning = TableRead & {
  readonly type: 'missing-index';
  /**
   * The columns the rows are found by: the columns a join binds and the
   * columns filtered for equality.
   */
  readonly columns: readonly string[];
  /** The rows each read scans. */
  readonly rows: number;
};

/**
 * No index returns the rows of a table in the requested order, so every read
 * sorts all of the matching rows before returning the first one. A limit does
 * not reduce the rows read.
 */
export type FullSortWarning = TableRead & {
  readonly type: 'full-sort';
  readonly orderBy: Ordering;
  /** The estimated number of rows each read sorts. */
  readonly rows: number;
};

/**
 * The best plan the planner found is still estimated to process a lot of
 * rows. Only reported when every table the query reads has statistics, since
 * the estimates are guesses otherwise.
 */
export type HighCostWarning = {
  readonly type: 'high-cost';
  readonly table: string;
  /** The estimated number of rows the plan processes. */
  readonly cost: number;
};

export type PlanWarning =
  | MissingIndexWarning
  | FullSortWarning
  | HighCostWarning;

/**
 * Warns about what the cost model reports the plans in `plans` will do
 * slowly. Call this after planning: it estimates each graph's current plan.
 *
 * @param table The table of the query that `plans` was built for.
 */
export function collectPlanWarnings(
  table: string,
  plans: Plans,
  thresholds: PlanWarningThresholds,
): PlanWarning[] {
  const warnings = new Map<string, PlanWarning>();
  const state = {warnings, estimated: true};
  const cost = visit(plans, [], thresholds, state);
  const result = [...warnings.values()];
  if (state.estimated && thresholds.cost > 0 && cost >= thresholds.cost) {
    result.push({type: 'high-cost', table, cost: Math.round(cost)});
  }
  return result;
}

type VisitState = {
  readonly warnings: Map<string, PlanWarning>;
  /** Whether every access read a table with statistics. */
  estimated: boolean;
};

/**
 * Collects the warnings for `plans` and its subplans, and returns the
 * estimated number of rows they process.
 */
function visit(
  plans: Plans,
  path: readonly string[],
  thresholds: PlanWarningThresholds,
  state: VisitState,
): number {
  const estimate = plans.plan.estimateCurrentPlan();
  for (const connection of plans.plan.connections) {
    for (const access of connection.accesses()) {
      if (access.plan?.tableRows === undefined) {
        state.estimated = false;
      } else if (thresholds.rows > 0) {
        for (const warning of accessWarnings(
          connection,
          access,
          access.plan,
          access.plan.tableRows,
          path,
          thresholds.rows,
        )) {
          state.warnings.set(JSON.stringify(warning), warning);
        }
      }
    }
  }

  // A `related` subquery runs once for each row of its parent.
  let subCost = 0;
  for (const [alias, subPlans] of Object.entries(plans.subPlans)) {
    subCost += visit(subPlans, [...path, alias], thresholds, state);
  }
  const rowsOut =
    estimate.limit === undefined
      ? estimate.returnedRows
      : Math.min(estimate.returnedRows, estimate.limit);
  return (
    estimate.startupCost + estimate.cost + estimate.scanEst + rowsOut * subCost
  );
}

function* accessWarnings(
  connection: PlannerConnection,
  access: ConnectionAccess,
  plan: AccessPlan,
  tableRows: number,
  path: readonly string[],
  minRows: number,
): Iterable<MissingIndexWarning | FullSortWarning> {
  const constrained = Object.keys(access.constraint ?? {});
  const columns = unique([...constrained, ...equalityColumns(access.filters)]);
  const read: TableRead = {
    table: connection.table,
    path,
    perRow: constrained.length > 0,
    suggestedIndex: unique([
      ...columns,
      ...connection.sort.map(([column]) => column),
    ]),
  };

  // A scan that stops at a limit reads an unknown share of the table, unless
  // it runs per row: then even a limit of one (an EXISTS) scans the table
  // for every row that has no match.
  if (
    plan.access === 'scan' &&
    columns.length > 0 &&
    (read.perRow || connection.limit === undefined) &&
    tableRows >= minRows
  ) {
    yield {type: 'missing-index', ...read, columns, rows: tableRows};
  }

  if (plan.sort === 'full' && access.rows >= minRows) {
    yield {
      type: 'full-sort',
      ...read,
      orderBy: connection.sort,
      rows: Math.round(access.rows),
    };
  }
}

/**
 * The columns that the top-level conjuncts of `condition` compare for
 * equality with a value, which an index lookup can use.
 */
function equalityColumns(condition: Condition | undefined): string[] {
  switch (condition?.type) {
    case 'simple': {
      const {left, op} = condition;
      return (op === '=' || op === 'IS' || op === 'IN') &&
        left.type === 'column'
        ? [left.name]
        : [];
    }
    case 'and':
      return condition.conditions.flatMap(equalityColumns);
    default:
      return [];
  }
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}
