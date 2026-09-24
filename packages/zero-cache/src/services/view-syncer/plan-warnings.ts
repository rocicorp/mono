import type {PlanWarning} from '../../../../zql/src/planner/planner-warnings.ts';

/**
 * Explains a {@link PlanWarning} for a zero-cache operator, in terms of the
 * SQLite replica, whose indexes come from the upstream database.
 */
export function planWarningMessage(warning: PlanWarning): string {
  switch (warning.type) {
    case 'missing-index': {
      const {columns, rows, perRow} = warning;
      return (
        `${perRow ? 'Each lookup' : 'Each read'} of ${location(warning)} ` +
        `by ${list(columns)} scans all ~${count(rows)} rows because no ` +
        `index covers ${list(columns)}` +
        `${perRow ? ', and it runs once per parent row' : ''}. ` +
        suggestion(warning)
      );
    }
    case 'full-sort': {
      const {orderBy, rows, perRow} = warning;
      const ordering = orderBy
        .map(([column, direction]) => `${column} ${direction}`)
        .join(', ');
      return (
        `Each read of ${location(warning)} sorts ~${count(rows)} rows for ` +
        `ORDER BY ${ordering}, reading every matching row before returning ` +
        `the first, so a limit does not reduce the rows read` +
        `${perRow ? ', and it runs once per parent row' : ''}. ` +
        suggestion(warning)
      );
    }
    case 'high-cost':
      return (
        `The best plan found for the query is estimated to process ` +
        `~${count(warning.cost)} rows.`
      );
  }
}

function location({table, path}: {table: string; path: readonly string[]}) {
  return path.length === 0
    ? table
    : `${table} (related ${path.map(alias => `'${alias}'`).join(' > ')})`;
}

function suggestion({
  table,
  suggestedIndex,
}: {
  table: string;
  suggestedIndex: readonly string[];
}) {
  return `Consider adding an index on ${table} (${suggestedIndex.join(', ')}) upstream.`;
}

function list(columns: readonly string[]) {
  return columns.join(', ');
}

function count(n: number) {
  return Math.round(n).toLocaleString('en-US');
}
